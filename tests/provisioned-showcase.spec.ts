import { test, expect } from '@grafana/plugin-e2e';

// Grafana lazy-renders panel DOM nodes only when they enter the viewport.
// Strategy: scroll to each row's pixel position, wait for rendering, then assert.
// Dashboard layout: 6 rows, ~50 grid units tall, each unit ≈ 30px → total ~1500px.

test.describe('Provisioned multi-format showcase', () => {
  test('loads the multi-format showcase dashboard', async ({ page }) => {
    test.setTimeout(60000);

    await page.goto('/d/kafka-multi-format-showcase');

    await expect(page.getByText('Kafka Financial Transaction Monitor', { exact: true })).toBeVisible();

    // Row 1 (y=0–9) — visible on initial load
    await expect(page.getByText('Transaction Value', { exact: true })).toBeVisible();
    await expect(page.getByText('Authorization Outcomes', { exact: true })).toBeVisible();

    // Rows 2–3 (y=9–25, ~270–750px)
    await page.evaluate(() => window.scrollTo(0, 400));
    await page.waitForTimeout(1500);
    await expect(page.getByText('Payment Amounts Over Time', { exact: true })).toBeVisible({ timeout: 8000 });
    await expect(page.getByText('Avg Transaction Size', { exact: true })).toBeVisible({ timeout: 8000 });

    await page.evaluate(() => window.scrollTo(0, 750));
    await page.waitForTimeout(1500);
    await expect(page.getByText('Largest Transaction', { exact: true })).toBeVisible({ timeout: 8000 });
    await expect(page.getByText('Revenue by Customer Tier', { exact: true })).toBeVisible({ timeout: 8000 });
    await expect(page.getByText('Transaction Volume by Service', { exact: true })).toBeVisible({ timeout: 8000 });

    // Row 4 (y=25–37, ~750–1110px)
    await page.evaluate(() => window.scrollTo(0, 1000));
    await page.waitForTimeout(1500);
    await expect(page.getByText('Live Payment Feed', { exact: true })).toBeVisible({ timeout: 8000 });

    // Rows 5–6 (y=37–50, ~1110–1500px)
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(1500);
    await expect(page.getByText('Avro: Transaction Ledger', { exact: true })).toBeVisible({ timeout: 8000 });
    await expect(page.getByText('Protobuf: Payment Analytics', { exact: true })).toBeVisible({ timeout: 8000 });
    await expect(page.getByText('Plaintext: Raw Audit Trail', { exact: true })).toBeVisible({ timeout: 8000 });
  });
});
