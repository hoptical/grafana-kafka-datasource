import { test, expect } from '@grafana/plugin-e2e';
import { ChildProcess, spawn } from 'child_process';
import { accessSync, constants } from 'fs';
import type { Page } from '@playwright/test';

// Alert rules evaluate through backend QueryData, not Grafana Live. These tests
// call /api/ds/query directly, the same path the alerting engine uses.

function startTransactionalProducer(topic: string): { producer: ChildProcess; exitPromise: Promise<void> } {
  const producerPath = './dist/producer';
  try {
    accessSync(producerPath, constants.X_OK);
  } catch (err) {
    throw new Error(`Kafka producer executable not found or not executable at path: ${producerPath}`);
  }

  const transactionalId = `txn-alert-e2e-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
  const args = [
    '-broker',
    'localhost:9094',
    '-topic',
    topic,
    '-connect-timeout',
    '5000',
    '-num-partitions',
    '1',
    '-interval',
    '200',
    '-shape',
    'flat',
    '-transactional-id',
    transactionalId,
  ];
  const producer = spawn(producerPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });
  producer.stderr?.on('data', (data) => {
    console.error('[Producer stderr]', data.toString());
  });

  const exitPromise = new Promise<void>((resolve, reject) => {
    producer.on('error', (err) => reject(err));
    producer.on('exit', () => resolve());
  });

  return { producer, exitPromise };
}

type SnapshotResult = { error?: string; rows: number };

async function datasourceUid(page: Page, name: string): Promise<string> {
  // The provisioning file does not pin a uid, so resolve it from Grafana.
  const response = await page.request.get(`/api/datasources/name/${encodeURIComponent(name)}`);
  expect(response.ok()).toBeTruthy();
  return (await response.json()).uid;
}

async function querySnapshot(page: Page, datasourceUid: string, topic: string): Promise<SnapshotResult> {
  const now = Date.now();
  const response = await page.request.post('/api/ds/query', {
    data: {
      from: String(now - 10 * 60 * 1000),
      to: String(now),
      queries: [
        {
          refId: 'A',
          datasource: { uid: datasourceUid },
          topicName: topic,
          partition: 'all',
          messageFormat: 'json',
          autoOffsetReset: 'latest',
          timestampMode: 'message',
          selectedField: 'value2',
        },
      ],
    },
  });
  const body = await response.json();
  const result = body.results?.A ?? {};
  const frames: Array<{ data: { values: unknown[][] } }> = result.frames ?? [];
  const rows = frames.reduce((sum, frame) => sum + (frame.data.values[0]?.length ?? 0), 0);
  return { error: result.error, rows };
}

test.describe('Kafka Alerting snapshot', () => {
  test('returns committed values from an idle transactional topic', async ({ readProvisionedDataSource, page }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const uid = await datasourceUid(page, ds.name);
    const topic = `alert-txn-${Date.now()}`;
    const { producer, exitPromise } = startTransactionalProducer(topic);

    try {
      // Wait until the producer has committed some messages.
      await expect
        .poll(async () => (await querySnapshot(page, uid, topic)).rows, { timeout: 30000, intervals: [1000] })
        .toBeGreaterThan(3);
    } finally {
      producer.kill();
      await Promise.race([exitPromise.catch(() => {}), new Promise((resolve) => setTimeout(resolve, 2000))]);
    }

    // With the producer stopped, the partition ends with a COMMIT marker. The
    // snapshot must stop there instead of waiting for the deadline, and the
    // idle reader must close quickly enough for the frames to be returned.
    const result = await querySnapshot(page, uid, topic);
    expect(result.error).toBeUndefined();
    expect(result.rows).toBeGreaterThan(3);
  });
});
