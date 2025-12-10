// Shared test utilities for Kafka datasource e2e tests
import { expect } from '@grafana/plugin-e2e';
import type { Page } from '@playwright/test';
import type { PanelEditPage } from '@grafana/plugin-e2e';

// Common regex patterns for data validation
export const TIMESTAMP_REGEX = /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/;
export const NUMERIC_REGEX = /^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/;
export const ALERTS_PATTERN = 'severity';

// Common column headers to check
export const COMMON_COLUMN_HEADERS = ['time', 'offset', 'partition', 'host.ip'];

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
export async function verifyColumnHeadersVisible(
  page: Page,
  headers: string[] = COMMON_COLUMN_HEADERS
): Promise<void> {
  for (const header of headers) {
    await expect(page.getByRole('columnheader', { name: header })).toBeVisible();
  }
}