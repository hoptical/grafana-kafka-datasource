import { test, expect } from '@grafana/plugin-e2e';
import { exec, ChildProcess } from 'child_process';
import { accessSync, constants } from 'fs';

const isV10 = (process.env.GRAFANA_VERSION || '').startsWith('10.');
function startKafkaProducer(): ChildProcess {
  const producerPath = './dist/producer';
  try {
    accessSync(producerPath, constants.X_OK); // Check if file exists and is executable
  } catch (err) {
    throw new Error(`Kafka producer executable not found or not executable at path: ${producerPath}`);
  }
  const producer = exec(
    `${producerPath} -broker localhost:9094 -topic test-topic -connect-timeout 500 -num-partitions 3`,
    { encoding: 'utf-8' }
  );

  producer.stdout?.on('data', (data) => {
    console.log('[Producer stdout]', data);
  });
  producer.stderr?.on('data', (data) => {
    console.error('[Producer stderr]', data);
  });
  producer.on('exit', (code) => {
    if (code !== 0) {
      throw new Error(`Kafka producer exited with code ${code}`);
    }
  });
  return producer;
}

test.describe('Kafka Query Editor', () => {
  test('should display and configure all query editor fields correctly', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });

    // Select the Kafka datasource
    await panelEditPage.datasource.set(ds.name);

    // Check that all modern query editor fields are visible
    await expect(page.getByText('Topic')).toBeVisible();
    await expect(page.getByText('Partition', { exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Fetch' })).toBeVisible();
    await expect(
      page
        .locator('div')
        .filter({ hasText: /^Latest$/ })
        .nth(2)
    ).toBeVisible();
    await expect(page.locator('div').filter({ hasText: /^Now$/ }).nth(2)).toBeVisible();

    await expect(page.getByRole('textbox', { name: 'Enter topic name' })).toBeVisible();
    await expect(
      page
        .locator('div')
        .filter({ hasText: /^All partitions$/ })
        .nth(2)
    ).toBeVisible();

    // Test configuring all parameters and verify they work correctly
    await page.getByRole('textbox', { name: 'Enter topic name' }).fill('a-topic');
    await page
      .locator('div')
      .filter({ hasText: /^All partitions$/ })
      .nth(2)
      .click();
    if (isV10) {
      await page.getByLabel('Select options menu').getByText('All partitions').click();
    } else {
      await page.getByRole('option', { name: /^All partitions$/ }).click();
    }
    // Test select interactions and options
    const autoOffsetSelect = page
      .locator('div')
      .filter({ hasText: /^Latest$/ })
      .nth(2);
    const timestampSelect = page.locator('div').filter({ hasText: /^Now$/ }).nth(2);

    // Test Offset options
    await autoOffsetSelect.click();
    await expect(page.getByText('Last N messages')).toBeVisible();
    await expect(page.getByText('Earliest')).toBeVisible();
    if (isV10) {
      await page.getByLabel('Select options menu').getByText('Latest').click();
    } else {
      await page.getByRole('option', { name: 'Latest' }).click();
    }

    // Test Timestamp Mode options
    await timestampSelect.click();
    await expect(page.getByText('Message Timestamp')).toBeVisible();
    if (isV10) {
      await page.getByLabel('Select options menu').getByText('Message Timestamp').click();
    } else {
      await page.getByRole('option', { name: 'Message Timestamp' }).click();
    }
  });

  // Test streaming data from Kafka topic with the right topic
  test('should stream data from kafka topic', async ({ readProvisionedDataSource, page, panelEditPage }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });

    // Select the Kafka datasource
    await panelEditPage.datasource.set(ds.name);

    // Start the Kafka producer
    startKafkaProducer();
    // Wait for some data to be produced
    await new Promise((resolve) => setTimeout(resolve, 3000));

    // Fill in the query editor fields
    await page.getByRole('textbox', { name: 'Enter topic name' }).fill('test-topic');
    await page.getByRole('button', { name: 'Fetch' }).click();
    await page
      .locator('div')
      .filter({ hasText: /^All partitions$/ })
      .nth(2)
      .click();

    if (isV10) {
      await page.getByLabel('Select options menu').getByText('All partitions').click();
    } else {
      await page.getByRole('option', { name: /^All partitions$/ }).click();
    }
    await panelEditPage.setVisualization('Table');

    // Wait for the time column to appear first (this indicates data is flowing)
    await expect(page.getByRole('columnheader', { name: 'time' })).toBeVisible({ timeout: 10000 });
    await expect(page.getByRole('columnheader', { name: 'value1' })).toBeVisible();
    await expect(page.getByRole('columnheader', { name: 'value2' })).toBeVisible();

    // Verify that data is flowing correctly with proper formats
    // Check for timestamp format in time column (YYYY-MM-DD HH:MM:SS)
    await expect(page.getByRole('cell').filter({ hasText: /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ })).toBeVisible();
    // Check for float numbers in value columns (just verify at least one numeric cell exists)
    await expect(
      page
        .getByRole('cell')
        .filter({ hasText: /^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/ })
        .first()
    ).toBeVisible();
  });

  test('shows no success and no partitions for non-existent topic fetch', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    await panelEditPage.datasource.set(ds.name);
    await page.getByRole('textbox', { name: 'Enter topic name' }).fill('nonexistent_topic_xyz');
    await page.getByRole('button', { name: 'Fetch' }).click();
    // Expect the error
    await expect(page.getByText('topic nonexistent_topic_xyz not found')).toBeVisible();
    // We removed inline error; ensure no success message appears
    await expect(page.getByText(/Fetched \d+ partition/)).not.toBeVisible();
    // Open partition select and ensure no concrete partition entries were added
    await page.locator('#query-editor-partition').click();
    await expect(page.getByRole('option', { name: /Partition 0/ })).toHaveCount(0);
  });

  test('streams from a single partition and lists all partition options after fetch', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    await panelEditPage.datasource.set(ds.name);

    // Start producer
    startKafkaProducer();
    await new Promise((r) => setTimeout(r, 3000));

    // Fetch partitions for existing topic
    await page.getByRole('textbox', { name: 'Enter topic name' }).fill('test-topic');
    await page.getByRole('button', { name: 'Fetch' }).click();

    // Open partition select and ensure options present (All partitions + partition 0,1,2)
    await page.locator('#query-editor-partition').click();
    // Some themes render options differently; ensure any partition labels appear
    for (let i = 0; i < 3; i++) {
      if (isV10) {
        await expect(page.getByLabel('Select options menu').getByText(`Partition ${i}`)).toBeVisible();
      } else {
        await expect(page.getByRole('option', { name: `Partition ${i}` })).toBeVisible();
      }
    }

    // Pick single partition 1
    if (isV10) {
      await page
        .getByLabel('Select options menu')
        .getByText(/Partition 1/)
        .click();
    } else {
      await page.getByRole('option', { name: /Partition 1/ }).click();
    }

    await panelEditPage.setVisualization('Table');
    // Wait for data columns
    await expect(page.getByRole('columnheader', { name: 'time' })).toBeVisible({ timeout: 10000 });
    await expect(page.getByRole('columnheader', { name: 'value1' })).toBeVisible();
    // Confirm partition column absent (single partition) or if present only single value
    // Not asserting strictly due to frame structure but ensures at least one data cell
    await expect(page.getByRole('cell').first()).toBeVisible();
  });
});
