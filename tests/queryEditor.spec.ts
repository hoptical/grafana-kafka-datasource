import { test, expect } from '@grafana/plugin-e2e';
import { exec } from 'child_process';

test.describe('Kafka Streaming', () => {
  test('should stream data from kafka topic', async ({ 
    readProvisionedDataSource,
    page, 
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });

    // Select the Kafka datasource
    await panelEditPage.datasource.set(ds.name);

    // Start the Kafka producer
    const _ = exec('go run example/go/producer.go -broker localhost:9092 -topic test', { encoding: 'utf-8' });

    await page.getByTestId('query-editor-row').getByRole('textbox').fill('test');
    await page.getByTestId('query-editor-row').getByRole('spinbutton').fill('0');
    await panelEditPage.setVisualization('Table');
    
    // Wait for data to appear in the panel - check individual column headers
    console.log('Checking for column headers...');
    
    // Wait for the time column to appear first (this indicates data is flowing)
    await expect(page.getByRole('columnheader', { name: 'time' })).toBeVisible({ timeout: 5000 });
    await expect(page.getByRole('columnheader', { name: 'value1' })).toBeVisible();
    await expect(page.getByRole('columnheader', { name: 'value2' })).toBeVisible();

    // Verify that data is flowing correctly with proper formats
    // Check for timestamp format in time column (YYYY-MM-DD HH:MM:SS)
    await expect(page.getByRole('cell').filter({ hasText: /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ })).toBeVisible();
    // Check for float numbers in value columns (just verify at least one numeric cell exists)
    await expect(page.getByRole('cell').filter({ hasText: /^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/ }).first()).toBeVisible();
  });
});
