/// <reference types="node" />

import { test, expect } from '@grafana/plugin-e2e';
import { Locator, Page } from '@playwright/test';
import { ChildProcess, spawn } from 'child_process';
import { accessSync, constants } from 'fs';
import {
  openPartitionSelector,
  selectAllPartitionsOption,
  setTableVisualization,
  verifyPanelDataContains,
  verifyColumnHeadersVisible,
} from './test-utils';

type ProducerHandle = {
  producer: ChildProcess;
  exitPromise: Promise<void>;
};

function startLineProtocolKafkaProducer(topic: string): ProducerHandle {
  const producerPath = './dist/producer';
  try {
    accessSync(producerPath, constants.X_OK);
  } catch (err) {
    throw new Error(`Kafka producer executable not found or not executable at path: ${producerPath}`);
  }

  const args = [
    '-broker',
    'localhost:9094',
    '-topic',
    topic,
    '-connect-timeout',
    '5000',
    '-num-partitions',
    '3',
    '-interval',
    '300',
    '-format',
    'lineprotocol',
    '-key-format',
    'none',
  ];

  const producer = spawn(producerPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });

  producer.stdout?.on('data', (data) => {
    console.log('[Line Protocol Producer stdout]', data.toString());
  });
  producer.stderr?.on('data', (data) => {
    console.error('[Line Protocol Producer stderr]', data.toString());
  });

  const exitPromise = new Promise<void>((resolve, reject) => {
    producer.on('error', (err) => reject(err));
    producer.on('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`Line Protocol Kafka producer exited with code ${code}`));
      } else {
        resolve();
      }
    });
  });

  return { producer, exitPromise };
}

async function findMessageFormatSelector(page: Page): Promise<Locator | null> {
  const messageFormatApproaches = [
    page
      .locator('div')
      .filter({ hasText: /^JSON$/ })
      .nth(2),
    page.getByText('JSON').locator('../..'),
    page.locator('.css-1eu65zc').filter({ hasText: /JSON/ }),
    page.getByText('JSON').filter({ hasText: /^JSON$/ }),
    page.locator('button').filter({ hasText: /^JSON$/ }),
    page.getByText('Message Format').locator('..').locator('button').first(),
    page.locator('[data-testid*="select"]'),
  ];

  for (const approach of messageFormatApproaches) {
    if (await approach.isVisible({ timeout: 1000 })) {
      return approach;
    }
  }

  return null;
}

async function selectMessageFormat(page: Page, optionName: string): Promise<void> {
  await expect(page.getByText('Message Format')).toBeVisible({ timeout: 5000 });

  const foundSelector = await findMessageFormatSelector(page);
  expect(foundSelector).not.toBeNull();

  await foundSelector!.first().click();

  const option = page
    .getByLabel('Select options menu')
    .getByText(optionName)
    .or(page.getByRole('option', { name: optionName }));
  await expect(option.first()).toBeVisible({ timeout: 5000 });
  await option.first().click();
}

async function findTimestampPrecisionSelector(page: Page): Promise<Locator | null> {
  const selectorApproaches = [
    page.getByText('Timestamp Precision').locator('..').locator('button').first(),
    page
      .getByRole('combobox')
      .filter({ hasText: /Auto-detect|Nanoseconds|Microseconds|Milliseconds|Seconds/ })
      .first(),
    page
      .locator('div')
      .filter({ hasText: /^Auto-detect$/ })
      .first(),
  ];

  for (const approach of selectorApproaches) {
    if (await approach.isVisible({ timeout: 1000 })) {
      return approach;
    }
  }

  return null;
}

async function selectTimestampPrecision(page: Page, optionName: string): Promise<void> {
  await expect(page.getByText('Timestamp Precision')).toBeVisible({ timeout: 5000 });

  const selector = await findTimestampPrecisionSelector(page);
  expect(selector).not.toBeNull();

  await selector!.first().click();

  const option = page
    .getByLabel('Select options menu')
    .getByText(optionName)
    .or(page.getByRole('option', { name: optionName }));
  await expect(option.first()).toBeVisible({ timeout: 5000 });
  await option.first().click();
}

test.describe.serial('Kafka Query Editor - Line Protocol Tests', () => {
  test('should expose Line Protocol format and timestamp-precision selector', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    await panelEditPage.datasource.set(ds.name);

    await setTableVisualization(panelEditPage);

    // Fill a topic so the editor renders the format selector.
    await page.getByRole('textbox', { name: 'Enter topic name' }).fill(`lp-${Date.now()}`);

    await selectMessageFormat(page, 'Line Protocol');

    // Selecting Line Protocol should reveal the Timestamp Precision selector.
    await selectTimestampPrecision(page, 'Seconds');

    // The three Line Protocol filter inputs should appear once Line Protocol
    // is selected, and editing each one should run its handler without error.
    // Target the InlineField labels (associated with the inputs via id/htmlFor)
    // rather than the example placeholder text, which is illustrative and may
    // change without affecting behaviour.
    const measurementFilter = page.getByLabel('Measurement filter', { exact: true });
    const fieldFilter = page.getByLabel('Field filter', { exact: true });
    const tagFilter = page.getByLabel('Tag filter', { exact: true });

    await expect(measurementFilter).toBeVisible({ timeout: 5000 });
    await expect(fieldFilter).toBeVisible({ timeout: 5000 });
    await expect(tagFilter).toBeVisible({ timeout: 5000 });

    await measurementFilter.fill('Breaker Data');
    await fieldFilter.fill('PT Primary');
    await tagFilter.fill('Building=DCM102');

    await expect(measurementFilter).toHaveValue('Breaker Data');
    await expect(fieldFilter).toHaveValue('PT Primary');
    await expect(tagFilter).toHaveValue('Building=DCM102');
  });

  test('should stream line protocol data from kafka topic', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    await panelEditPage.datasource.set(ds.name);

    const topic = `lp-stream-${Date.now()}`;
    const { producer, exitPromise } = startLineProtocolKafkaProducer(topic);

    try {
      await new Promise((resolve) => setTimeout(resolve, 3000));

      await setTableVisualization(panelEditPage);

      await page.getByRole('textbox', { name: 'Enter topic name' }).fill(topic);
      await page.getByText(topic).click();

      await selectMessageFormat(page, 'Line Protocol');
      await selectTimestampPrecision(page, 'Seconds');

      const measurementFilter = page.getByLabel('Measurement filter', { exact: true });
      const fieldFilter = page.getByLabel('Field filter', { exact: true });
      const tagFilter = page.getByLabel('Tag filter', { exact: true });

      await expect(measurementFilter).toBeVisible({ timeout: 5000 });
      await expect(fieldFilter).toBeVisible({ timeout: 5000 });
      await expect(tagFilter).toBeVisible({ timeout: 5000 });

      await measurementFilter.fill('Breaker Data, Last Trip');
      await fieldFilter.fill('PT Primary, Last trip event Timestamp, Firmware revision, Main capability of device');
      await tagFilter.fill('Building=DCM102, Device_tag=-XQ202');

      await page.getByRole('button', { name: 'Fetch' }).click();

      await openPartitionSelector(page);
      await selectAllPartitionsOption(page);

      await verifyColumnHeadersVisible(page, ['Time', '_measurement', '_field']);

      await verifyPanelDataContains(panelEditPage, [
        /Breaker Data/,
        /Last Trip/,
        /PT Primary/,
        /Last trip event Timestamp/,
        /XXXXXX/,
        /MAIN_CAP/,
      ]);
    } finally {
      producer.kill();
      await Promise.race([exitPromise.catch(() => {}), new Promise((resolve) => setTimeout(resolve, 2000))]);
    }
  });
});
