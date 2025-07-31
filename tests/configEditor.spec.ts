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

    // Check the security protocol combobox
    await page.locator('div').filter({ hasText: /^PLAINTEXT$/ }).nth(2).click();
    await page.getByText('SASL_SSL', { exact: true }).click();
    
    //Check Authentication section
    await expect(page.getByRole('heading', { name: 'Authentication' })).toBeVisible();
  
    // Check TLS fields (checkboxes and textareas)
    await expect(page.getByText('Skip TLS Verification')).toBeVisible();
    await expect(page.getByText('Self-signed Certificate')).toBeVisible();
    await expect(page.getByText('TLS Client Authentication')).toBeVisible();
    await expect(page.getByRole('textbox', { name: 'SASL Username' })).toBeVisible();
    await expect(page.getByPlaceholder('SASL Password')).toBeVisible();

    // Check Advanced Settings section
    await expect(page.getByRole('heading', { name: 'Advanced Settings' })).toBeVisible();
    await page.getByRole('button', { name: 'Expand section Advanced' }).click();
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

    await page.getByRole('button', { name: 'Expand section Advanced' }).click();

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

    // Save & test the data source - should fail due to unreachable servers
    await expect(configPage.saveAndTest()).not.toBeOK();
  });


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
    await page.locator('div').filter({ hasText: /^PLAINTEXT$/ }).nth(2).click();
    await page.getByText('SASL_SSL', { exact: true }).click();
    await page.waitForTimeout(1000); // Wait for conditional rendering
    await page.locator('div').filter({ hasText: /^PLAIN$/ }).nth(2).click();
    await page.getByText('SCRAM-SHA-256', { exact: true }).click();
    await page.getByRole('textbox', { name: 'SASL Username' }).fill('testuser');
    await page.getByPlaceholder('SASL Password').fill('testpass');
    await page.locator('div').filter({ hasText: /^Skip TLS Verificationⓘ$/ }).locator('div span').click();
    await page.locator('div').filter({ hasText: /^Self-signed Certificateⓘ$/ }).locator('div span').click();
    await page.locator('div').filter({ hasText: /^TLS Client Authenticationⓘ$/ }).locator('div span').click();
    await page.getByRole('textbox', { name: 'Server Name' }).fill('my-server');
    await page.getByPlaceholder('Begins with -----BEGIN CERTIFICATE-----').nth(0).fill('test-ca-cert');
    await page.getByPlaceholder('Begins with -----BEGIN CERTIFICATE-----').nth(1).fill('test-client-cert');
    await page.getByPlaceholder('Begins with -----BEGIN PRIVATE KEY-----').fill('test-client-key');
    await page.getByRole('button', { name: 'Expand section Advanced' }).click();
    await page.getByRole('textbox', { name: 'Log Level' }).fill('debug');
    await page.getByRole('spinbutton', { name: 'Healthcheck Timeout (ms)' }).fill('3000');

    // Save the configuration
    await expect(configPage.saveAndTest()).not.toBeOK();

    // Verify values are preserved
    await expect(page.getByRole('textbox', { name: 'Client ID' })).toHaveValue('my-client-id');
    await expect(page.locator('div').filter({ hasText: /^Skip TLS Verificationⓘ$/ }).locator('div span')).toBeChecked();
    await expect(page.locator('div').filter({ hasText: /^Self-signed Certificateⓘ$/ }).locator('div span')).toBeChecked();
    await expect(page.locator('div').filter({ hasText: /^TLS Client Authenticationⓘ$/ }).locator('div span')).toBeChecked();
    await expect(page.getByRole('textbox', { name: 'Server Name' })).toHaveValue('my-server');

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
    
    await page.locator('div').filter({ hasText: /^PLAINTEXT$/ }).nth(2).click();
    await page.getByText('SASL_PLAINTEXT', { exact: true }).click();
    await page.waitForTimeout(500);
    
    await page.locator('div').filter({ hasText: /^PLAIN$/ }).nth(2).click();
    await page.getByText('SCRAM-SHA-512', { exact: true }).click();
    
    await page.getByRole('textbox', { name: 'SASL Username' }).fill('testuser');
    await page.getByPlaceholder('SASL Password').fill('testpass');


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
    
    await page.locator('div').filter({ hasText: /^PLAINTEXT$/ }).nth(2).click();
    await page.getByText('SASL_PLAINTEXT', { exact: true }).click();
    await page.waitForTimeout(500);
    
    await page.locator('div').filter({ hasText: /^PLAIN$/ }).nth(2).click();
    await page.getByText('SCRAM-SHA-512', { exact: true }).click();
    
    await page.getByRole('textbox', { name: 'SASL Username' }).fill('testuser');
    await page.getByPlaceholder('SASL Password').fill('wrongpass');

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
    
    await page.locator('div').filter({ hasText: /^PLAINTEXT$/ }).nth(2).click();
    await page.getByText('SASL_SSL', { exact: true }).click();
    await page.waitForTimeout(500);
    
    await page.locator('div').filter({ hasText: /^PLAIN$/ }).nth(2).click();
    await page.getByText('SCRAM-SHA-512', { exact: true }).click();
    
    await page.getByRole('textbox', { name: 'SASL Username' }).fill('testuser');
    await page.getByPlaceholder('SASL Password').fill('testpass');
    await page.locator('div').filter({ hasText: /^Self-signed Certificateⓘ$/ }).locator('div span').click();
    await page.getByPlaceholder('Begins with -----BEGIN CERTIFICATE-----').nth(0).fill(caCert);


    // Save the configuration
    await expect(configPage.saveAndTest()).toBeOK();
  });
});
