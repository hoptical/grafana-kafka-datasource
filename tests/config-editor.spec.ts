import { test, expect } from '@grafana/plugin-e2e';

test.describe('Kafka Config Editor', () => {
  test('should allow configuring datasource with correct servers and without SASL', async ({ 
    createDataSourceConfigPage, 
    readProvisionedDataSource, 
    page 
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Fill in the required fields
    await page.getByRole('textbox', { name: 'broker1:9092, broker2:9092' }).fill('kafka:29092');
    await page.getByRole( 'textbox', { name: '<debug|error>'}).fill('error');

    // Save & test the data source
    await expect(configPage.saveAndTest()).toBeOK();
  });

  test('should must not allow configuring datasource with incorrect servers', async ({ 
    createDataSourceConfigPage, 
    readProvisionedDataSource, 
    page 
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Fill in the required fields
    await page.getByRole('textbox', { name: 'broker1:9092, broker2:9092' }).fill('wrong-server:29092');

    // Save & test the data source
    await expect(configPage.saveAndTest()).not.toBeOK();
  });
});
