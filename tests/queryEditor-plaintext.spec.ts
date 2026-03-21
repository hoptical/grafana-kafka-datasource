import { test, expect } from '@grafana/plugin-e2e';
import { ChildProcess, spawn } from 'child_process';
import { accessSync, constants } from 'fs';
import { setTableVisualization, verifyColumnHeadersVisible, verifyPanelDataContains } from './test-utils';

type ProducerOptions = {
  topic: string;
  connectTimeoutMs?: number;
  numPartitions?: number;
  intervalMs?: number;
};

function startKafkaProducer(options: ProducerOptions): { producer: ChildProcess; exitPromise: Promise<void> } {
  const producerPath = './dist/producer';
  try {
    accessSync(producerPath, constants.X_OK);
  } catch {
    throw new Error(`Kafka producer executable not found or not executable at path: ${producerPath}`);
  }

  // Producer currently supports json/avro/protobuf only.
  // For plaintext query-mode coverage, we produce JSON bytes and consume them as plaintext.
  const args = [
    '-broker',
    'localhost:9094',
    '-topic',
    options.topic,
    '-connect-timeout',
    String(options.connectTimeoutMs ?? 5000),
    '-num-partitions',
    String(options.numPartitions ?? 1),
    '-interval',
    String(options.intervalMs ?? 500),
    '-format',
    'json',
  ];

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

test.describe('Kafka Query Editor - Plaintext Tests', () => {
  test('should stream message bytes as a single message field when plaintext format is selected', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    await panelEditPage.datasource.set(ds.name);

    const topic = `test-plaintext-${Date.now()}`;
    const { producer, exitPromise } = startKafkaProducer({
      topic,
      numPartitions: 1,
      intervalMs: 300,
    });

    try {
      await new Promise((resolve) => setTimeout(resolve, 2000));

      await setTableVisualization(panelEditPage);

      // Configure topic and fetch partitions.
      await page.getByRole('textbox', { name: 'Enter topic name' }).fill(topic);
      await page.getByRole('button', { name: 'Fetch' }).click();

      // Select Plaintext message format.
      const messageFormatSelector = page
        .getByText('Message Format')
        .locator('..')
        .locator('button')
        .first()
        .or(
          page
            .getByRole('combobox')
            .filter({ hasText: /JSON|Avro|Protobuf|Plaintext/ })
            .first()
        )
        .or(
          page
            .locator('div')
            .filter({ hasText: /^JSON$/ })
            .first()
        );

      await expect(messageFormatSelector.first()).toBeVisible({ timeout: 8000 });
      await messageFormatSelector.first().click();

      const plaintextOption = page
        .getByLabel('Select options menu')
        .getByText('Plaintext')
        .or(page.getByRole('option', { name: /^Plaintext$/ }));

      await expect(plaintextOption.first()).toBeVisible({ timeout: 5000 });
      await plaintextOption.first().click();

      // Validate table headers for plaintext mode.
      await verifyColumnHeadersVisible(page, ['time', 'offset', 'message']);

      // Validate plaintext message values are present in the panel data.
      await verifyPanelDataContains(panelEditPage, [/"host"|"metrics"|"alerts"|"value1"|"value2"/]);
    } finally {
      producer.kill();
      await Promise.race([
        exitPromise.catch(() => {
          // Ignore non-zero exit during test cleanup.
        }),
        new Promise((resolve) => setTimeout(resolve, 2000)),
      ]);
    }
  });
});
