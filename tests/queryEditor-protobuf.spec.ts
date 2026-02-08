import { test, expect } from '@grafana/plugin-e2e';
import { Page, Locator } from '@playwright/test';
import { ChildProcess, spawn } from 'child_process';
import { accessSync, constants, readFileSync } from 'fs';
import path from 'path';
import { verifyPanelDataContains, verifyColumnHeadersVisible } from './test-utils';

interface ProtobufProducerOptions {
  topic: string;
  schemaRegistry?: boolean;
}

function startProtobufKafkaProducer({ topic, schemaRegistry }: ProtobufProducerOptions): {
  producer: ChildProcess;
  exitPromise: Promise<void>;
} {
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
    '500',
    '-num-partitions',
    '3',
    '-format',
    'protobuf',
  ];
  if (schemaRegistry) {
    args.push('-schema-registry', 'http://localhost:8081');
  }
  const producer = spawn(producerPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });

  producer.stdout?.on('data', (data) => {
    console.log('[Protobuf Producer stdout]', data.toString());
  });
  producer.stderr?.on('data', (data) => {
    console.error('[Protobuf Producer stderr]', data.toString());
  });

  const exitPromise = new Promise<void>((resolve, reject) => {
    producer.on('error', (err) => reject(err));
    producer.on('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`Protobuf Kafka producer exited with code ${code}`));
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

async function selectProtobufMessageFormat(page: Page): Promise<void> {
  await expect(page.getByText('Message Format')).toBeVisible({ timeout: 5000 });
  const foundSelector = await findMessageFormatSelector(page);
  expect(foundSelector).not.toBeNull();
  await foundSelector!.first().click();
  await page.getByText('Protobuf').click();
}

function getProtobufSchemaSourceLocator(page: Page): Locator {
  return page
    .getByText('Protobuf Schema Source')
    .or(page.getByText('Schema Source'))
    .or(page.locator('label').filter({ hasText: /schema.*source/i }));
}

async function findProtobufSchemaSourceSelector(page: Page): Promise<Locator | null> {
  const schemaSourceApproaches = [
    page.locator('[data-testid="protobuf-schema-source"]'),
    page
      .locator('div')
      .filter({ hasText: /^Schema Registry$/ })
      .nth(2),
    page.getByText('Protobuf Schema Source').locator('..').locator('button'),
    page.getByRole('combobox').filter({ hasText: /Schema Registry|Inline Schema/ }),
    page.locator('button').filter({ hasText: /Schema Registry|Inline/ }),
    page.getByText('Schema Registry').locator('..').locator('.css-1eu65zc'),
    page.locator('button').filter({ hasText: /Schema Registry/ }),
  ];

  for (const approach of schemaSourceApproaches) {
    if (await approach.isVisible({ timeout: 1000 })) {
      return approach;
    }
  }
  return null;
}

async function selectInlineSchema(page: Page): Promise<void> {
  const schemaSourceSelector = await findProtobufSchemaSourceSelector(page);
  expect(schemaSourceSelector).not.toBeNull();

  await expect(schemaSourceSelector!.first()).toBeVisible({ timeout: 5000 });
  await schemaSourceSelector!.first().click();
  await page.getByText('Inline Schema', { exact: true }).click();
}

test.describe.serial('Kafka Query Editor - Protobuf Tests', () => {
  test('should configure Protobuf message format and validate schema', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    await panelEditPage.datasource.set(ds.name);

    await page.getByRole('textbox', { name: 'Enter topic name' }).fill('test-protobuf-topic');
    await selectProtobufMessageFormat(page);

    await expect(getProtobufSchemaSourceLocator(page).first()).toBeVisible({ timeout: 8000 });

    await selectInlineSchema(page);

    const schemaTextarea = page
      .locator('textarea[placeholder*="Protobuf"]')
      .or(page.getByRole('textbox', { name: /schema/i }));

    await expect(schemaTextarea.first()).toBeVisible({ timeout: 5000 });

    const fixturePath = path.resolve(__dirname, 'fixtures', 'test-protobuf-schema.proto');
    const fixtureContents = readFileSync(fixturePath, 'utf8');

    const fileInputLocator = page.locator('input[type="file"]');
    await fileInputLocator.first().setInputFiles(fixturePath);

    const textareaValue = await schemaTextarea.first().inputValue();
    expect(textareaValue.trim()).toBe(fixtureContents.trim());
  });

  test('should stream Protobuf data from kafka topic with schema registry', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    await panelEditPage.datasource.set(ds.name);

    const { producer, exitPromise } = startProtobufKafkaProducer({
      topic: 'test-protobuf-schema-topic',
      schemaRegistry: true,
    });

    try {
      await new Promise((resolve) => setTimeout(resolve, 3000));

      try {
        await panelEditPage.setVisualization('Table');
      } catch (error) {
        console.log('Visualization picker blocked by overlay, continuing...');
      }

      await page.getByRole('textbox', { name: 'Enter topic name' }).fill('test-protobuf-schema-topic');
      await page.getByText('test-protobuf-schema-topic').click();
      await selectProtobufMessageFormat(page);

      const partitionSelector = page
        .locator('div')
        .filter({ hasText: /^All partitions$/ })
        .nth(2)
        .or(page.locator('#query-editor-partition'));

      await expect(partitionSelector.first()).toBeVisible({ timeout: 5000 });
      await partitionSelector.first().click();

      const allPartitionsOption = page
        .getByLabel('Select options menu')
        .getByText('All partitions')
        .or(page.getByRole('option', { name: /^All partitions$/ }));
      await allPartitionsOption.first().click();

      await verifyColumnHeadersVisible(page);
      await verifyPanelDataContains(panelEditPage);
    } finally {
      producer.kill();
      await Promise.race([exitPromise.catch(() => {}), new Promise((resolve) => setTimeout(resolve, 2000))]);
    }
  });
});
