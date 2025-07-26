import { test, expect } from '@grafana/plugin-e2e';
import { exec, ChildProcess } from 'child_process';
import { accessSync, constants } from 'fs';

function startKafkaProducer(): ChildProcess {
    const producerPath = './dist/producer';
  try {
    accessSync(producerPath, constants.X_OK); // Check if file exists and is executable
  } catch (err) {
    throw new Error(`Kafka producer executable not found or not executable at path: ${producerPath}`);
  }
  const producer = exec(`${producerPath} -broker localhost:9094 -topic test -connect-timeout 500`, { encoding: 'utf-8' });

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
    await expect(page.getByLabel('Topic')).toBeVisible();
    await expect(page.getByLabel('Partition')).toBeVisible();
    await expect(page.locator('div').filter({ hasText: /^Auto offset reset$/ })).toBeVisible();
    await expect(page.locator('div').filter({ hasText: /^Timestamp Mode$/ })).toBeVisible();

    // Check placeholders
    await expect(page.getByPlaceholder('Enter topic name')).toBeVisible();
    await expect(page.getByPlaceholder('0')).toBeVisible();

    // Test configuring all parameters and verify they work correctly
    await page.getByLabel('Topic').fill('test-topic');
    await page.getByLabel('Partition').fill('2');

    // Test combobox interactions and options
    const autoOffsetCombobox = page.locator('div').filter({ hasText: /^Auto offset reset$/ }).getByRole('combobox');
    const timestampCombobox = page.locator('div').filter({ hasText: /^Timestamp Mode$/ }).getByRole('combobox');

    // Test Auto offset reset options
    await autoOffsetCombobox.click();
    await expect(page.getByText('From the last')).toBeVisible();
    await expect(page.getByText('Latest')).toBeVisible();
    await page.getByText('From the last 100').click();

    // Test Timestamp Mode options
    await timestampCombobox.click();
    await expect(page.getByText('Now')).toBeVisible();
    await expect(page.getByText('Message Timestamp')).toBeVisible();
    await page.getByText('Message Timestamp').click();

    // Verify all values are preserved
    await expect(page.getByLabel('Topic')).toHaveValue('test-topic');
    await expect(page.getByLabel('Partition')).toHaveValue('2');
    await expect(autoOffsetCombobox).toHaveValue('From the last 100');
    await expect(timestampCombobox).toHaveValue('Message Timestamp');
  });

  test('should validate partition input as numeric', async ({ 
    readProvisionedDataSource,
    page, 
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });

    // Select the Kafka datasource
    await panelEditPage.datasource.set(ds.name);

    const partitionField = page.getByLabel('Partition');
    
    // Should accept valid numbers
    await partitionField.fill('5');
    await expect(partitionField).toHaveValue('5');
    
    // Should enforce minimum value of 0 (convert negative to 0)
    await partitionField.fill('-1');
    await partitionField.blur(); // Trigger onChange event
    await expect(partitionField).toHaveValue('0');
    
    // Should accept large partition numbers
    await partitionField.fill('999');
    await expect(partitionField).toHaveValue('999');
  });

  test('should stream data from kafka topic', async ({ 
    readProvisionedDataSource,
    page, 
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });

    // Select the Kafka datasource
    await panelEditPage.datasource.set(ds.name);

    // Start the Kafka producer
    startKafkaProducer();
    // Wait for some data to be produced
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Fill in the query editor fields
    await page.getByLabel('Topic').fill('test');
    await page.getByLabel('Partition').fill('0');
    
    await panelEditPage.setVisualization('Table');

    // Wait for the time column to appear first (this indicates data is flowing)
    await expect(page.getByRole('columnheader', { name: 'time' })).toBeVisible({ timeout: 10000 });
    await expect(page.getByRole('columnheader', { name: 'value1' })).toBeVisible();
    await expect(page.getByRole('columnheader', { name: 'value2' })).toBeVisible();

    // Verify that data is flowing correctly with proper formats
    // Check for timestamp format in time column (YYYY-MM-DD HH:MM:SS)
    await expect(page.getByRole('cell').filter({ hasText: /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ })).toBeVisible();
    // Check for float numbers in value columns (just verify at least one numeric cell exists)
    await expect(page.getByRole('cell').filter({ hasText: /^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/ }).first()).toBeVisible();
  });
});
