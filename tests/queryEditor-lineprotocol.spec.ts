import { test, expect } from '@grafana/plugin-e2e';
import { setTableVisualization } from './test-utils';

test.describe('Kafka Query Editor - Line Protocol Tests', () => {
  test('should expose Line Protocol format and timestamp-precision selector', async ({
    readProvisionedDataSource,
    page,
    panelEditPage,
  }) => {
    const ds = await readProvisionedDataSource({ fileName: 'datasource.yaml' });
    await panelEditPage.datasource.set(ds.name);

    await setTableVisualization(panelEditPage);

    // Fill a topic so the editor renders the format selector.
    await page.getByRole('textbox', { name: 'Enter topic name' }).fill(`lp-${Date.now()}`);

    // Open the Message Format select.
    const messageFormatSelector = page
      .getByText('Message Format')
      .locator('..')
      .locator('button')
      .first()
      .or(
        page
          .getByRole('combobox')
          .filter({ hasText: /JSON|Avro|Protobuf|Plaintext|Line Protocol/ })
          .first()
      )
      .or(
        page
          .locator('div')
          .filter({ hasText: /^JSON$/ })
          .first()
      );
    await expect(messageFormatSelector.first()).toBeVisible({ timeout: 8000 });
    await messageFormatSelector.first().click();

    // Confirm the Line Protocol option exists in the dropdown.
    const lineProtocolOption = page
      .getByLabel('Select options menu')
      .getByText('Line Protocol')
      .or(page.getByRole('option', { name: /^Line Protocol$/ }));
    await expect(lineProtocolOption.first()).toBeVisible({ timeout: 5000 });
    await lineProtocolOption.first().click();

    // Selecting Line Protocol should reveal the Timestamp Precision selector.
    await expect(page.getByText('Timestamp Precision')).toBeVisible({ timeout: 5000 });

    // Switching precision should not throw — open the dropdown and pick Seconds.
    const precisionSelector = page
      .getByText('Timestamp Precision')
      .locator('..')
      .locator('button')
      .first()
      .or(
        page
          .getByRole('combobox')
          .filter({ hasText: /Auto-detect|Nanoseconds|Microseconds|Milliseconds|Seconds/ })
          .first()
      );
    await expect(precisionSelector.first()).toBeVisible({ timeout: 5000 });
    await precisionSelector.first().click();

    const secondsOption = page
      .getByLabel('Select options menu')
      .getByText('Seconds')
      .or(page.getByRole('option', { name: /^Seconds$/ }));
    await expect(secondsOption.first()).toBeVisible({ timeout: 5000 });
    await secondsOption.first().click();

    // The three Line Protocol filter inputs should appear once Line Protocol
    // is selected, and editing each one should run its handler without error.
    const measurementFilter = page.getByPlaceholder(/Breaker Data/);
    const fieldFilter = page.getByPlaceholder(/PT Primary/);
    const tagFilter = page.getByPlaceholder(/Building=DCM/);

    await expect(measurementFilter).toBeVisible({ timeout: 5000 });
    await expect(fieldFilter).toBeVisible({ timeout: 5000 });
    await expect(tagFilter).toBeVisible({ timeout: 5000 });

    await measurementFilter.fill('Breaker Data');
    await fieldFilter.fill('PT Primary');
    await tagFilter.fill('Building=DCM102');

    await expect(measurementFilter).toHaveValue('Breaker Data');
    await expect(fieldFilter).toHaveValue('PT Primary');
    await expect(tagFilter).toHaveValue('Building=DCM102');
  });
});
