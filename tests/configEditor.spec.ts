import { test, expect } from '@grafana/plugin-e2e';

test.describe('Kafka Config Editor', () => {
  test('should allow configuring datasource without SASL', async ({ 
    createDataSourceConfigPage, 
    readProvisionedDataSource, 
    page 
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Fill in the required fields
    await page.getByRole('textbox', { name: 'Bootstrap Servers' }).fill('kafka:29092');
    await page.getByRole('textbox', { name: 'Log Level' }).fill('error');
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill('2000');

    // Save & test the data source
    await expect(configPage.saveAndTest()).toBeOK();
  });
});
