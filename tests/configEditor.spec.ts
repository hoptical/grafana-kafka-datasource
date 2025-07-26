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
    await page.getByRole('textbox', { name: 'Bootstrap Servers' }).fill('kafka:9092');
    await page.getByRole('textbox', { name: 'Log Level' }).fill('error');
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill('2000');

    // Save & test the data source
    await expect(configPage.saveAndTest()).toBeOK();
  });

  test('should display all configuration fields correctly', async ({ 
    createDataSourceConfigPage, 
    readProvisionedDataSource, 
    page 
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Check Connection section
    await expect(page.getByRole('heading', { name: 'Connection' })).toBeVisible();
    await expect(page.getByRole('textbox', { name: 'Bootstrap Servers' })).toBeVisible();
    await expect(page.getByRole('textbox', { name: 'Security Protocol' })).toBeVisible();

    // Check Authentication section
    await expect(page.getByRole('heading', { name: 'Authentication' })).toBeVisible();
    await expect(page.getByRole('textbox', { name: 'SASL Mechanisms' })).toBeVisible();
    await expect(page.getByRole('textbox', { name: 'SASL Username' })).toBeVisible();
    await expect(page.getByPlaceholder('SASL Password')).toBeVisible();
    await expect(page.getByPlaceholder('secure json field (backend only)')).toBeVisible();

    // Check Advanced Settings section
    await expect(page.getByRole('heading', { name: 'Advanced Settings' })).toBeVisible();
    await expect(page.getByRole('textbox', { name: 'Log Level' })).toBeVisible();
    await expect(page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' })).toBeVisible();
  });

  test('should validate healthcheck timeout input and enforce non-negative values', async ({ 
    createDataSourceConfigPage, 
    readProvisionedDataSource, 
    page 
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Test numeric input validation
    const timeoutField = page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' });
    
    // Should accept valid positive numbers
    await timeoutField.fill('1000');
    await expect(timeoutField).toHaveValue('1000');
    
    // Should accept decimal numbers
    await timeoutField.fill('1500.5');
    await expect(timeoutField).toHaveValue('1500.5');
    
    // Should accept zero
    await timeoutField.fill('0');
    await expect(timeoutField).toHaveValue('0');
    
    // Should enforce minimum value of 0 (convert negative to 0)
    await timeoutField.fill('-100');
    await timeoutField.blur(); // Trigger onChange
    await expect(timeoutField).toHaveValue('0');
    
    // Should handle large numbers
    await timeoutField.fill('60000');
    await expect(timeoutField).toHaveValue('60000');
  });

  test('should fail when configuring datasource with unreachable servers', async ({ 
    createDataSourceConfigPage, 
    readProvisionedDataSource, 
    page 
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Fill in unreachable broker servers
    await page.getByRole('textbox', { name: 'Bootstrap Servers' }).fill('unreachable-host:9092,another-bad-host:9093');
    await page.getByRole('textbox', { name: 'Log Level' }).fill('error');
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill('1000');

    // Save & test the data source - should fail due to unreachable servers
    await expect(configPage.saveAndTest()).not.toBeOK();
  });

  test('should preserve configuration values after save', async ({ 
    createDataSourceConfigPage, 
    readProvisionedDataSource, 
    page 
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Fill in all configuration fields
    const bootstrapServers = 'kafka:9092';
    const securityProtocol = 'SASL_SSL';
    const saslMechanisms = 'PLAIN';
    const saslUsername = 'testuser';
    const logLevel = 'debug';
    const healthcheckTimeout = '3000';

    await page.getByRole('textbox', { name: 'Bootstrap Servers' }).fill(bootstrapServers);
    await page.getByRole('textbox', { name: 'Security Protocol' }).fill(securityProtocol);
    await page.getByRole('textbox', { name: 'SASL Mechanisms' }).fill(saslMechanisms);
    await page.getByRole('textbox', { name: 'SASL Username' }).fill(saslUsername);
    await page.getByRole('textbox', { name: 'Log Level' }).fill(logLevel);
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill(healthcheckTimeout);

    // Save the configuration
    await configPage.saveAndTest();

    // Verify values are preserved
    await expect(page.getByRole('textbox', { name: 'Bootstrap Servers' })).toHaveValue(bootstrapServers);
    await expect(page.getByRole('textbox', { name: 'Security Protocol' })).toHaveValue(securityProtocol);
    await expect(page.getByRole('textbox', { name: 'SASL Mechanisms' })).toHaveValue(saslMechanisms);
    await expect(page.getByRole('textbox', { name: 'SASL Username' })).toHaveValue(saslUsername);
    await expect(page.getByRole('textbox', { name: 'Log Level' })).toHaveValue(logLevel);
    await expect(page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' })).toHaveValue(healthcheckTimeout);
  });

  test('should allow configuring datasource with SASL_PLAINTEXT and SCRAM-SHA-512', async ({
    createDataSourceConfigPage,
    readProvisionedDataSource,
    page
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Fill in the required fields for SASL_PLAINTEXT with SCRAM-SHA-512
    await page.getByRole('textbox', { name: 'Bootstrap Servers' }).fill('kafka:29092');
    await page.getByRole('textbox', { name: 'Security Protocol' }).fill('SASL_PLAINTEXT');
    await page.getByRole('textbox', { name: 'SASL Mechanisms' }).fill('SCRAM-SHA-512');
    await page.getByRole('textbox', { name: 'SASL Username' }).fill('testuser');
    await page.getByPlaceholder('SASL Password').fill('testpass');
    await page.getByRole('textbox', { name: 'Log Level' }).fill('info');
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill('2000');

    // Save & test the data source
    await expect(configPage.saveAndTest()).toBeOK();
  });

  test('Must not allow configuring datasource with SASL_PLAINTEXT with wrong credentials', async ({
    createDataSourceConfigPage,
    readProvisionedDataSource,
    page
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Fill in the required fields for SASL_PLAINTEXT with SCRAM-SHA-512
    await page.getByRole('textbox', { name: 'Bootstrap Servers' }).fill('kafka:29092');
    await page.getByRole('textbox', { name: 'Security Protocol' }).fill('SASL_PLAINTEXT');
    await page.getByRole('textbox', { name: 'SASL Mechanisms' }).fill('SCRAM-SHA-512');
    await page.getByRole('textbox', { name: 'SASL Username' }).fill('testuser');
    await page.getByPlaceholder('SASL Password').fill('wrongpass');
    await page.getByRole('textbox', { name: 'Log Level' }).fill('info');
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill('2000');

    // Save & test the data source
    await expect(configPage.saveAndTest()).not.toBeOK();
  });

});
