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
    await page.getByRole('textbox', { name: 'Servers' }).fill('localhost:29092');
    await page.getByLabel('Log Level').fill('error');
    await page.getByLabel('Healthcheck Timeout (ms)').fill('2000');

    // Save & test the data source
    await expect(configPage.saveAndTest()).toBeOK();
  });
});
