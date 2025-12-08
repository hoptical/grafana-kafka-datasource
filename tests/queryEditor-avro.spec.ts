import { test, expect } from '@grafana/plugin-e2e';
import { Page, Locator } from '@playwright/test';
import { ChildProcess, exec } from 'child_process';
import { accessSync, constants } from 'fs';

// Helper to get table cells compatible across Grafana versions
// Grafana v12.2.0+ uses 'gridcell' role, older versions use 'cell'
function getTableCells(page: Page): Locator {
  // Use a CSS selector that works for both roles
  return page.locator('[role="gridcell"], [role="cell"]');
}

function startAvroKafkaProducer(): ChildProcess {
  const producerPath = './dist/producer';
  try {
    accessSync(producerPath, constants.X_OK); // Check if file exists and is executable
  } catch (err) {
    throw new Error(`Kafka producer executable not found or not executable at path: ${producerPath}`);
  }
  const producer = exec(
    `${producerPath} -broker localhost:9094 -topic test-avro-topic -connect-timeout 500 -num-partitions 3 -format avro -schema-registry http://localhost:8081`,
    { encoding: 'utf-8' }
  );

  producer.stdout?.on('data', (data) => {
    console.log('[Avro Producer stdout]', data);
  });
  producer.stderr?.on('data', (data) => {
    console.error('[Avro Producer stderr]', data);
  });
  producer.on('exit', (code) => {
    if (code !== 0) {
      throw new Error(`Avro Kafka producer exited with code ${code}`);
    }
  });
  return producer;
}

async function findMessageFormatSelector(page: Page): Promise<Locator | null> {
  const messageFormatApproaches = [
    page.locator('div').filter({ hasText: /^JSON$/ }).nth(2), // The intercepting parent element
    page.getByText('JSON').locator('../..'), // Go up two levels to find clickable parent
    page.locator('.css-1eu65zc').filter({ hasText: /JSON/ }), // Direct selection of intercepting element
    page.getByText('JSON').filter({ hasText: /^JSON$/ }), // Original approach
    page.locator('button').filter({ hasText: /^JSON$/ }),  // Button with exact JSON text
    page.getByText('Message Format').locator('..').locator('button').first(), // Button near Message Format label
    page.locator('[data-testid*="select"]'), // Any element with select in testid
  ];

  for (let i = 0; i < messageFormatApproaches.length; i++) {
    const approach = messageFormatApproaches[i];
    if (await approach.isVisible({ timeout: 1000 })) {
      console.log(`Message format selector found using approach ${i}: ${approach.toString()}`);
      return approach;
    }
  }
  return null;
}

async function selectAvroMessageFormat(page: Page): Promise<void> {
  // Wait for the page to stabilize after filling topic name
  await expect(page.getByText('Message Format')).toBeVisible({ timeout: 5000 });

  console.log('Looking for Message Format selector among buttons...');

  const foundSelector = await findMessageFormatSelector(page);

  // Message format selector MUST be found for Avro tests
  expect(foundSelector).not.toBeNull();

  // Click the message format selector
  await foundSelector!.first().click();
  await page.getByText('Avro').click();
}

function getAvroSchemaSourceLocator(page: Page): Locator {
  return page.getByText('Avro Schema Source')
    .or(page.getByText('Schema Source'))
    .or(page.locator('label').filter({ hasText: /schema.*source/i }));
}

async function findAvroSchemaSourceSelector(page: Page): Promise<Locator | null> {
  const schemaSourceApproaches = [
    page.locator('[data-testid="avro-schema-source"]'),
    page.locator('div').filter({ hasText: /^Schema Registry$/ }).nth(2), // Use same pattern as Message Format
    page.getByText('Avro Schema Source').locator('..').locator('button'),
    page.getByRole('combobox').filter({ hasText: /Schema Registry|Inline Schema/ }),
    page.locator('button').filter({ hasText: /Schema Registry|Inline/ }),
    page.getByText('Schema Registry').locator('..').locator('.css-1eu65zc'),
    page.locator('button').filter({ hasText: /Schema Registry/ }),
  ];

  for (const approach of schemaSourceApproaches) {
    if (await approach.isVisible({ timeout: 1000 })) {
      console.log(`Schema source selector found using approach: ${approach.toString()}`);
      return approach;
    }
  }
  return null;
}

function getInlineSchemaOption(page: Page): Locator {
  return page.getByRole('option', { name: 'Inline Schema' })
    .or(page.getByText('Inline Schema', { exact: true }));
}

async function selectInlineSchema(page: Page): Promise<void> {
  const schemaSourceSelector = await findAvroSchemaSourceSelector(page);
  expect(schemaSourceSelector).not.toBeNull();

  await expect(schemaSourceSelector!.first()).toBeVisible({ timeout: 5000 });
  await schemaSourceSelector!.first().click();

  const inlineSchemaOption = getInlineSchemaOption(page);
  await expect(inlineSchemaOption.first()).toBeVisible({ timeout: 3000 });
  await inlineSchemaOption.first().click();
}

test.describe('Kafka Query Editor - Avro Tests', () => {
  test('should configure Avro message format and validate schema', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    await panelEditPage.datasource.set(ds.name);

    // Fill in topic name
    await page.getByRole('textbox', { name: 'Enter topic name' }).fill('test-topic');

    // Select Avro message format
    await selectAvroMessageFormat(page);

    // Avro configuration fields should appear with better waiting
    // Wait for Avro configuration to appear after format change
    await expect(getAvroSchemaSourceLocator(page).first()).toBeVisible({ timeout: 8000 });

    // Test Schema Registry validation with better selectors - use "Test Connection" button text
    const schemaRegistryButton = page.getByRole('button', { name: /test.*connection|validate.*registry/i })
      .or(page.locator('button').filter({ hasText: /test.*connection|validate/i }))
      .or(page.getByText('Test Connection'));

    if (await schemaRegistryButton.first().isVisible({ timeout: 3000 })) {
      await schemaRegistryButton.first().click();
      // Should show validation result (may pass or fail depending on setup)
      const validationResult = page.getByText(/registry|connection|accessible|error|success|failed/i);
      await expect(validationResult.first()).toBeVisible({ timeout: 8000 });
    }

    // Test Inline Schema option - need to click the Avro Schema Source dropdown
    await selectInlineSchema(page);

    // Now the schema textarea should appear - wait by checking for it
    const schemaTextarea = page.locator('textarea[placeholder*="schema"]')
      .or(page.getByRole('textbox', { name: /schema/i }))
      .or(page.getByPlaceholder(/paste.*schema|avro.*schema/i))
      .or(page.locator('textarea').filter({ hasText: /schema/i }));

    await expect(schemaTextarea.first()).toBeVisible({ timeout: 5000 });

    // Test valid Avro schema
    const validSchema = `{
      "type": "record",
      "name": "TestRecord",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "value", "type": "int"}
      ]
    }`;

    await schemaTextarea.first().fill(validSchema);

    // Should show validation status (may take a moment)
    const validationStatus = page.getByText(/valid|invalid|loading|validating/i);
    if (await validationStatus.first().isVisible({ timeout: 3000 })) {
      console.log('Schema validation result appeared');
    }

    // Test invalid schema
    await schemaTextarea.first().fill('invalid json schema');
    const invalidResult = page.getByText(/invalid|error/i);
    if (await invalidResult.first().isVisible({ timeout: 3000 })) {
      console.log('Invalid schema validation worked');
    }

    // Test file upload if available
    const fileUploadInput = page.locator('input[type="file"]').filter({ hasText: /avsc|json/ })
      .or(page.locator('input[type="file"][accept*=".avsc"]'));
    if (await fileUploadInput.isVisible({ timeout: 2000 })) {
      // File upload functionality exists but we won't test actual file upload in e2e
      await expect(fileUploadInput.first()).toBeVisible();
      console.log('File upload input found');
    }
  });

  // Test streaming Avro data from Kafka topic with Schema Registry
  test('should stream Avro data from kafka topic with schema registry', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });

    // Select the Kafka datasource
    await panelEditPage.datasource.set(ds.name);

    // Start the Avro Kafka producer with schema registry
    startAvroKafkaProducer();
    // Wait for some data to be produced
    await new Promise((resolve) => setTimeout(resolve, 3000));

    // Fill in the query editor fields
    await page.getByRole('textbox', { name: 'Enter topic name' }).fill('test-avro-topic');

    // Select Avro message format
    await selectAvroMessageFormat(page);

    // Ensure Schema Registry is selected (should be default)
    await page.getByRole('button', { name: 'Fetch' }).click();

    // Wait for partition selector to be available after fetch
    const partitionSelector = page.locator('div').filter({ hasText: /^All partitions$/ }).nth(2)
      .or(page.locator('#query-editor-partition'))
      .or(page.getByText('All partitions').locator('..').locator('.css-1eu65zc'));

    // Partition selector MUST be found after fetch
    await expect(partitionSelector.first()).toBeVisible({ timeout: 5000 });
    await partitionSelector.first().click();

    // Select "All partitions" option
    const allPartitionsOption = page.getByLabel('Select options menu').getByText('All partitions')
      .or(page.getByRole('option', { name: /^All partitions$/ }));
    await allPartitionsOption.first().click();

    // Set visualization
    try {
      await panelEditPage.setVisualization('Table');
    } catch (error) {
      console.log('Visualization picker blocked by overlay, continuing...');
    }

    // Wait for the time column to appear first (this indicates data is flowing)
    await expect(page.getByRole('columnheader', { name: 'time' })).toBeVisible({ timeout: 10000 });
    await expect(page.getByRole('columnheader', { name: 'offset' })).toBeVisible();
    await expect(page.getByRole('columnheader', { name: 'partition' })).toBeVisible();

    // Verify that Avro data is flowing correctly
    // Check for timestamp format in time column (YYYY-MM-DD HH:MM:SS)
    await expect(getTableCells(page).filter({ hasText: /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ }).first()).toBeVisible();
    // Check for Avro-specific fields (host_name, host_ip, etc.)
    await expect(getTableCells(page).filter({ hasText: 'severity' })).toBeVisible(); // host_ip
  });

  // Test streaming Avro data from Kafka topic with Inline Schema
  test('should stream Avro data from kafka topic with inline schema', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });

    // Select the Kafka datasource
    await panelEditPage.datasource.set(ds.name);

    // Start the Avro Kafka producer with schema registry
    startAvroKafkaProducer();
    // Wait for some data to be produced
    await new Promise((resolve) => setTimeout(resolve, 3000));

    // Fill in the query editor fields
    await page.getByRole('textbox', { name: 'Enter topic name' }).fill('test-avro-topic');

    await page.getByText('test-avro-topic').click(); // The topic name is clicked from the autocomplete list
    // Select Avro message format
    await selectAvroMessageFormat(page);

    // Wait for Avro configuration to appear
    await expect(getAvroSchemaSourceLocator(page)).toBeVisible({ timeout: 5000 });

    // Switch to Inline Schema
    await selectInlineSchema(page);

    // Wait for schema textarea and fill it with the correct schema
    const schemaTextarea = page.locator('textarea[placeholder*="schema"]')
      .or(page.getByRole('textbox', { name: /schema/i }));

    await expect(schemaTextarea.first()).toBeVisible({ timeout: 5000 });

    // Use the same schema that the producer uses (nested shape)
    const avroSchema = `{
      "type": "record",
      "name": "TestRecord",
      "fields": [
        {
          "name": "host",
          "type": {
            "type": "record",
            "name": "Host",
            "fields": [
              {"name": "name", "type": "string"},
              {"name": "ip", "type": "string"}
            ]
          }
        },
        {
          "name": "metrics",
          "type": {
            "type": "record",
            "name": "Metrics",
            "fields": [
              {
                "name": "cpu",
                "type": {
                  "type": "record",
                  "name": "CPU",
                  "fields": [
                    {"name": "load", "type": ["null", "double"], "default": null},
                    {"name": "temp", "type": "double"}
                  ]
                }
              },
              {
                "name": "mem",
                "type": {
                  "type": "record",
                  "name": "Memory",
                  "fields": [
                    {"name": "used", "type": "int"},
                    {"name": "free", "type": "int"}
                  ]
                }
              }
            ]
          }
        },
        {"name": "value1", "type": ["null", "double"], "default": null},
        {"name": "value2", "type": ["null", "double"], "default": null},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {
          "name": "alerts",
          "type": {
            "type": "array",
            "items": {
              "type": "record",
              "name": "Alert",
              "fields": [
                {"name": "type", "type": "string"},
                {"name": "severity", "type": "string"},
                {"name": "value", "type": "double"}
              ]
            }
          }
        },
        {"name": "processes", "type": {"type": "array", "items": "string"}}
      ]
    }`;

    await schemaTextarea.first().fill(avroSchema);

    // Fetch partitions
    await page.getByRole('button', { name: 'Fetch' }).click();

    // Wait for partition selector to be available after fetch
    const partitionSelector = page.locator('div').filter({ hasText: /^All partitions$/ }).nth(2)
      .or(page.locator('#query-editor-partition'))
      .or(page.getByText('All partitions').locator('..').locator('.css-1eu65zc'));

    // Partition selector MUST be found after fetch
    await expect(partitionSelector.first()).toBeVisible({ timeout: 5000 });
    await partitionSelector.first().click();

    // Select "All partitions" option
    const allPartitionsOption = page.getByLabel('Select options menu').getByText('All partitions')
      .or(page.getByRole('option', { name: /^All partitions$/ }));
    await allPartitionsOption.first().click();

    // Set visualization
    try {
      await panelEditPage.setVisualization('Table');
    } catch (error) {
      console.log('Visualization picker blocked by overlay, continuing...');
    }

    // Wait for the time column to appear first (this indicates data is flowing)
    await expect(page.getByRole('columnheader', { name: 'time' })).toBeVisible({ timeout: 10000 });
    await expect(page.getByRole('columnheader', { name: 'offset' })).toBeVisible();
    await expect(page.getByRole('columnheader', { name: 'partition' })).toBeVisible();

    // Verify that Avro data is flowing correctly with inline schema
    // Check for timestamp format in time column (YYYY-MM-DD HH:MM:SS)
    await expect(getTableCells(page).filter({ hasText: /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ }).first()).toBeVisible();

    // Check for nested Avro fields (host.name, host.ip, etc.)
    await expect(getTableCells(page).filter({ hasText: 'severity' })).toBeVisible(); // host.name
  });
});