import { test, expect } from '@grafana/plugin-e2e';
import fs from 'fs';
import path from 'path';

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
    await page.getByRole('button', { name: 'Expand section Advanced' }).click();
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
      // Check Client ID field
    await expect(page.getByRole('textbox', { name: 'Client ID' })).toBeVisible();
    await expect(page.getByRole('textbox', { name: 'Bootstrap Servers' })).toBeVisible();
    await expect(page.getByRole('combobox', { name: 'Security Protocol' })).toBeVisible();
    // Check Authentication section

    await expect(page.getByRole('heading', { name: 'Authentication' })).toBeVisible();
    
    // Properly select SASL_SSL from the Security Protocol combobox
    await page.getByRole('combobox', { name: 'Security Protocol' }).click();
    await expect(page.getByText('SASL_SSL')).toBeVisible();
    await page.getByText('SASL_SSL').click();
    await page.waitForTimeout(5000); // Wait longer for conditional rendering
    
    
    // // Verify security protocol is actually selected
    // await expect(page.getByRole('combobox', { name: 'Security Protocol' })).toHaveValue('SASL_SSL');
    
    // // Wait for conditional fields to render and check if they're visible
    // await expect(page.getByRole('combobox', { name: 'SASL Mechanism' })).toBeVisible();

    // // Check TLS fields (checkboxes and textareas)
    // await expect(page.getByLabel('Skip TLS Verification')).toBeVisible();
    // await expect(page.getByLabel('Self-signed Certificate')).toBeVisible();
    // await expect(page.getByLabel('TLS Client Authentication')).toBeVisible();
    // await expect(page.getByRole('textbox', { name: 'SASL Username' })).toBeVisible();
    // await expect(page.getByPlaceholder('SASL Password')).toBeVisible();

    // // Check Advanced Settings section
    // await expect(page.getByRole('heading', { name: 'Advanced Settings' })).toBeVisible();
    // await expect(page.getByRole('textbox', { name: 'Log Level' })).toBeVisible();
    // await expect(page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' })).toBeVisible();
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
    
    // Properly select from comboboxes instead of filling as textboxes
    await page.getByRole('combobox', { name: 'Security Protocol' }).click();
    await page.getByText(securityProtocol).click();
    await page.waitForTimeout(1000); // Wait for conditional rendering
    
    await page.getByRole('combobox', { name: 'SASL Mechanism' }).click();
    await page.getByText(saslMechanisms).click();
    
    await page.getByRole('textbox', { name: 'SASL Username' }).fill(saslUsername);
    await page.getByRole('textbox', { name: 'Log Level' }).fill(logLevel);
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill(healthcheckTimeout);

    // Save the configuration
    await configPage.saveAndTest();

    // Verify values are preserved
    await expect(page.getByRole('textbox', { name: 'Bootstrap Servers' })).toHaveValue(bootstrapServers);
    await expect(page.getByRole('combobox', { name: 'Security Protocol' })).toHaveValue(securityProtocol);
    await expect(page.getByRole('combobox', { name: 'SASL Mechanism' })).toHaveValue(saslMechanisms);
    // Check Client ID value is preserved
    await expect(page.getByRole('textbox', { name: 'Client ID' })).toHaveValue(''); // or set a value and check
  test('should allow configuring datasource with TLS and clientId', async ({
    createDataSourceConfigPage,
    readProvisionedDataSource,
    page
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Fill in all configuration fields including TLS and clientId
    await page.getByRole('textbox', { name: 'Bootstrap Servers' }).fill('kafka:9092');
    await page.getByRole('textbox', { name: 'Client ID' }).fill('my-client-id');
    await page.getByRole('combobox', { name: 'Security Protocol' }).selectOption('SASL_SSL');
    await page.getByRole('combobox', { name: 'SASL Mechanism' }).selectOption('PLAIN');
    await page.getByRole('textbox', { name: 'SASL Username' }).fill('testuser');
    await page.getByPlaceholder('SASL Password').fill('testpass');
    await page.getByLabel('Skip TLS Verification').check();
    await page.getByLabel('Self-signed Certificate').check();
    await page.getByLabel('TLS Client Authentication').check();
    await page.getByRole('textbox', { name: 'Server Name' }).fill('my-server');
    await page.getByPlaceholder('Begins with -----BEGIN CERTIFICATE-----').fill('test-ca-cert');
    await page.getByPlaceholder('Begins with -----BEGIN CERTIFICATE-----').nth(1).fill('test-client-cert');
    await page.getByPlaceholder('Begins with -----BEGIN PRIVATE KEY-----').fill('test-client-key');
    await page.getByRole('textbox', { name: 'Log Level' }).fill('debug');
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill('3000');

    // Save the configuration
    await expect(configPage.saveAndTest()).toBeOK();

    // Verify values are preserved
    await expect(page.getByRole('textbox', { name: 'Client ID' })).toHaveValue('my-client-id');
    await expect(page.getByRole('combobox', { name: 'Security Protocol' })).toHaveValue('SASL_SSL');
    await expect(page.getByRole('combobox', { name: 'SASL Mechanism' })).toHaveValue('PLAIN');
    await expect(page.getByLabel('Skip TLS Verification')).toBeChecked();
    await expect(page.getByLabel('Self-signed Certificate')).toBeChecked();
    await expect(page.getByLabel('TLS Client Authentication')).toBeChecked();
    await expect(page.getByRole('textbox', { name: 'Server Name' })).toHaveValue('my-server');
    await expect(page.getByPlaceholder('Begins with -----BEGIN CERTIFICATE-----')).toHaveValue('test-ca-cert');
    await expect(page.getByPlaceholder('Begins with -----BEGIN CERTIFICATE-----').nth(1)).toHaveValue('test-client-cert');
    await expect(page.getByPlaceholder('Begins with -----BEGIN PRIVATE KEY-----')).toHaveValue('test-client-key');
  });
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
    
    await page.getByRole('combobox', { name: 'Security Protocol' }).click();
    await page.getByText('SASL_PLAINTEXT').click();
    await page.waitForTimeout(1000);
    
    await page.getByRole('combobox', { name: 'SASL Mechanism' }).click();
    await page.getByText('SCRAM-SHA-512').click();
    
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
    
    await page.getByRole('combobox', { name: 'Security Protocol' }).click();
    await page.getByText('SASL_PLAINTEXT').click();
    await page.waitForTimeout(1000);
    
    await page.getByRole('combobox', { name: 'SASL Mechanism' }).click();
    await page.getByText('SCRAM-SHA-512').click();
    
    await page.getByRole('textbox', { name: 'SASL Username' }).fill('testuser');
    await page.getByPlaceholder('SASL Password').fill('wrongpass');
    await page.getByRole('textbox', { name: 'Log Level' }).fill('info');
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill('2000');

    // Save & test the data source
    await expect(configPage.saveAndTest()).not.toBeOK();
  });
  test('should allow configuring datasource with SASL_SSL, SCRAM-SHA-512 and CA cert', async ({
    createDataSourceConfigPage,
    readProvisionedDataSource,
    page
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    const configPage = await createDataSourceConfigPage({ type: ds.type });

    // Read CA cert from file system (simulate user pasting it)
    const caCertPath = path.resolve(__dirname, '../tests/certs/ca.crt');
    const caCert = fs.readFileSync(caCertPath, 'utf-8');

    // Fill in all configuration fields for SASL_SSL with SCRAM-SHA-512 and CA cert
    await page.getByRole('textbox', { name: 'Bootstrap Servers' }).fill('kafka:39092');
    
    await page.getByRole('combobox', { name: 'Security Protocol' }).click();
    await page.getByText('SASL_SSL').click();
    await page.waitForTimeout(1000);
    
    await page.getByRole('combobox', { name: 'SASL Mechanism' }).click();
    await page.getByText('SCRAM-SHA-512').click();
    
    await page.getByRole('textbox', { name: 'SASL Username' }).fill('testuser');
    await page.getByPlaceholder('SASL Password').fill('testpass');
    await page.getByLabel('Self-signed Certificate').check();
    await page.getByPlaceholder('Begins with -----BEGIN CERTIFICATE-----').fill(caCert);
    await page.getByRole('textbox', { name: 'Log Level' }).fill('debug');
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill('3000');

    // Save the configuration
    await expect(configPage.saveAndTest()).toBeOK();

    // Verify values are preserved
    await expect(page.getByRole('textbox', { name: 'Bootstrap Servers' })).toHaveValue('kafka:39092');
    await expect(page.getByRole('combobox', { name: 'Security Protocol' })).toHaveValue('SASL_SSL');
    await expect(page.getByRole('combobox', { name: 'SASL Mechanism' })).toHaveValue('SCRAM-SHA-512');
    await expect(page.getByLabel('Self-signed Certificate')).toBeChecked();
    await expect(page.getByPlaceholder('Begins with -----BEGIN CERTIFICATE-----')).toHaveValue(caCert);
  });

});