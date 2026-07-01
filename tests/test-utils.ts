// Shared test utilities for Kafka datasource e2e tests
import { expect } from '@grafana/plugin-e2e';
import type { Page } from '@playwright/test';
import type { PanelEditPage } from '@grafana/plugin-e2e';

/**
 * Sets the panel visualization to 'Table', retrying once on failure.
 * Throws a clear, actionable error if both attempts fail so that subsequent
 * column-header assertions don't produce misleading "header not found" errors.
 */
export async function setTableVisualization(panelEditPage: PanelEditPage): Promise<void> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      await panelEditPage.setVisualization('Table');
      return;
    } catch (err) {
      lastError = err;
      if (attempt < 2) {
        // Brief pause to let any overlays or animations dismiss before retrying
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }
  const cause = lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(
    `setVisualization('Table') failed after 2 attempts — Table panel is not active.\n` +
      `Column header assertions require Table view and will fail without it.\n` +
      `Cause: ${cause}`
  );
}

// Common regex patterns for data validation
export const TIMESTAMP_REGEX = /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/;
export const NUMERIC_REGEX = /^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/;
export const ALERTS_PATTERN = 'severity';

// Common column headers to check
export const COMMON_COLUMN_HEADERS = ['time', 'offset', 'partition', 'host.ip'];
export const SINGLE_PARTITION_COLUMN_HEADERS = [...COMMON_COLUMN_HEADERS.filter((h) => h !== 'partition')];

// Helper function to verify panel data contains expected patterns
export async function verifyPanelDataContains(
  panelEditPage: PanelEditPage,
  patterns: (string | RegExp)[] = [TIMESTAMP_REGEX, NUMERIC_REGEX, ALERTS_PATTERN]
): Promise<void> {
  for (const pattern of patterns) {
    await expect(panelEditPage.panel.data.filter({ hasText: pattern })).not.toHaveCount(0);
  }
}

// Helper function to verify column headers are visible
export async function verifyColumnHeadersVisible(page: Page, headers: string[] = COMMON_COLUMN_HEADERS): Promise<void> {
  for (const header of headers) {
    await expect(page.getByRole('columnheader', { name: header })).toBeVisible();
  }
}

/**
 * Opens the partition selector dropdown using a set of fallback locators to
 * remain resilient across different Grafana/UI library versions.
 */
export async function openPartitionSelector(page: Page): Promise<void> {
  const partitionSelector = page
    .locator('div')
    .filter({ hasText: /^All partitions$/ })
    .nth(2)
    .or(page.locator('#query-editor-partition'))
    .or(page.getByText('All partitions').locator('..').locator('.css-1eu65zc'));

  await expect(partitionSelector.first()).toBeVisible({ timeout: 5000 });
  await partitionSelector.first().click();
}

/**
 * Selects the "All partitions" option from an already-open partition selector dropdown.
 */
export async function selectAllPartitionsOption(page: Page): Promise<void> {
  const allPartitionsOption = page
    .getByLabel('Select options menu')
    .getByText('All partitions')
    .or(page.getByRole('option', { name: /^All partitions$/ }));
  await allPartitionsOption.first().click();
}
