import { test, expect } from '@grafana/plugin-e2e';
import { ChildProcess, spawn } from 'child_process';
import { accessSync, constants } from 'fs';
import { verifyPanelDataContains, verifyColumnHeadersVisible, TIMESTAMP_REGEX, NUMERIC_REGEX } from './test-utils';

function startKafkaProducer(extraArgs: string[] = []): { producer: ChildProcess; exitPromise: Promise<void> } {
  const producerPath = './dist/producer';
  try {
    accessSync(producerPath, constants.X_OK);
  } catch (err) {
    throw new Error(`Kafka producer executable not found or not executable at path: ${producerPath}`);
  }

  const baseArgs = [
    '-broker',
    'localhost:9094',
    '-topic',
    'test-keys-topic',
    '-connect-timeout',
    '500',
    '-num-partitions',
    '1',
  ];
  const args = [...baseArgs, ...extraArgs];
  const producer = spawn(producerPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });

  producer.stdout?.on('data', (data) => {
    console.log('[Producer stdout]', data.toString());
  });
  producer.stderr?.on('data', (data) => {
    console.error('[Producer stderr]', data.toString());
  });

  const exitPromise = new Promise<void>((resolve, reject) => {
    producer.on('error', (err) => reject(err));
    producer.on('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`Kafka producer exited with code ${code}`));
      } else {
        resolve();
      }
    });
  });

  return { producer, exitPromise };
}

async function setupStreamingQuery(page: any, panelEditPage: any, readProvisionedDataSource: any) {
  const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
  await panelEditPage.datasource.set(ds.name);

  await page.getByRole('textbox', { name: 'Enter topic name' }).fill('test-keys-topic');
  await page.getByRole('button', { name: 'Fetch' }).click();
  await page.getByText('test-keys-topic').click();

  const partitionSelector = page
    .locator('div')
    .filter({ hasText: /^All partitions$/ })
    .nth(2)
    .or(page.locator('#query-editor-partition'))
    .or(page.getByText('All partitions').locator('..').locator('.css-1eu65zc'));

  await expect(partitionSelector.first()).toBeVisible({ timeout: 5000 });
  await partitionSelector.first().click();

  const allPartitionsOption = page
    .getByLabel('Select options menu')
    .getByText('All partitions')
    .or(page.getByRole('option', { name: /^All partitions$/ }));
  await allPartitionsOption.first().click();

  try {
    await panelEditPage.setVisualization('Table');
  } catch (error) {
    console.log('Skipping visualization setting to avoid timeout');
  }
}

async function selectKeyFormat(page: any, formatLabel: 'String' | 'JSON') {
  // The Key Format selector defaults to "None". Find it and change it.
  const keyFormatSelector = page
    .getByText('None', { exact: true })
    .locator('..')
    .locator('.css-1eu65zc')
    .or(
      page
        .getByRole('combobox')
        .filter({ hasText: /^None$/ })
        .first()
    )
    .or(
      page
        .locator('div')
        .filter({ hasText: /^None$/ })
        .nth(2)
    );

  await expect(keyFormatSelector.first()).toBeVisible({ timeout: 5000 });
  await keyFormatSelector.first().click();

  const option = page
    .getByLabel('Select options menu')
    .getByText(formatLabel)
    .or(page.getByRole('option', { name: formatLabel }));
  await option.first().click();
}

test.describe('Kafka Query Editor - Key Column Tests', () => {
  test('string key format adds a "key" column to the data frame', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const { producer, exitPromise } = startKafkaProducer(['-key-format', 'string']);
    try {
      await new Promise((resolve) => setTimeout(resolve, 3000));

      await setupStreamingQuery(page, panelEditPage, readProvisionedDataSource);

      // Change Key Format to String
      await selectKeyFormat(page, 'String');

      // Standard columns should still be present (no partition column since single partition)
      await verifyColumnHeadersVisible(page, ['time', 'offset', 'key']);

      // Data should still be flowing
      await verifyPanelDataContains(panelEditPage, [TIMESTAMP_REGEX, NUMERIC_REGEX]);
    } finally {
      producer.kill();
      await Promise.race([exitPromise.catch(() => {}), new Promise((resolve) => setTimeout(resolve, 2000))]);
    }
  });

  test('json key format adds "key.*" columns to the data frame', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const { producer, exitPromise } = startKafkaProducer(['-key-format', 'json']);
    try {
      await new Promise((resolve) => setTimeout(resolve, 3000));

      await setupStreamingQuery(page, panelEditPage, readProvisionedDataSource);

      // Change Key Format to JSON
      await selectKeyFormat(page, 'JSON');

      // Standard columns should still be present
      await verifyColumnHeadersVisible(page, ['time', 'offset', 'key.serverId', 'key.region']);

      // Data should still be flowing
      await verifyPanelDataContains(panelEditPage, [TIMESTAMP_REGEX, NUMERIC_REGEX]);
    } finally {
      producer.kill();
      await Promise.race([exitPromise.catch(() => {}), new Promise((resolve) => setTimeout(resolve, 2000))]);
    }
  });

  test('none key format does not add key columns (default behaviour)', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    // Producer sends no key (key-format=none)
    const { producer, exitPromise } = startKafkaProducer(['-key-format', 'none']);
    try {
      await new Promise((resolve) => setTimeout(resolve, 3000));

      // Use default Key Format (None) — no UI change needed
      await setupStreamingQuery(page, panelEditPage, readProvisionedDataSource);

      // Wait for data to stream
      await verifyColumnHeadersVisible(page, ['time', 'offset']);
      await verifyPanelDataContains(panelEditPage, [TIMESTAMP_REGEX, NUMERIC_REGEX]);

      // "key" column must NOT appear
      await expect(page.getByRole('columnheader', { name: 'key' })).toHaveCount(0);
      await expect(page.getByRole('columnheader', { name: 'key.serverId' })).toHaveCount(0);
    } finally {
      producer.kill();
      await Promise.race([exitPromise.catch(() => {}), new Promise((resolve) => setTimeout(resolve, 2000))]);
    }
  });
});
