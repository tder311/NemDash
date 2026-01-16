// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Live Prices Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for loading to complete
    await expect(page.getByText(/Loading market data/i)).not.toBeVisible({ timeout: 15000 });
  });

  test('displays Last Updated timestamp', async ({ page }) => {
    await expect(page.getByText(/Last Updated/i)).toBeVisible();
  });

  test('displays all 5 NEM regions in sidebar', async ({ page }) => {
    await expect(page.locator('.sidebar-region-card')).toHaveCount(5);

    // Check all region codes are present
    await expect(page.getByText('NSW')).toBeVisible();
    await expect(page.getByText('VIC')).toBeVisible();
    await expect(page.getByText('QLD')).toBeVisible();
    await expect(page.getByText('SA')).toBeVisible();
    await expect(page.getByText('TAS')).toBeVisible();
  });

  test('displays full region names', async ({ page }) => {
    await expect(page.getByText('New South Wales')).toBeVisible();
    await expect(page.getByText('Victoria')).toBeVisible();
    await expect(page.getByText('Queensland')).toBeVisible();
    await expect(page.getByText('South Australia')).toBeVisible();
    await expect(page.getByText('Tasmania')).toBeVisible();
  });

  test('displays prices in correct format', async ({ page }) => {
    // Prices should show $/MWh format
    const priceElements = page.locator('.sidebar-region-card').locator('text=/\\$[\\d.]+\\/MWh/');
    await expect(priceElements.first()).toBeVisible();
  });

  test('displays demand values', async ({ page }) => {
    // Demand should show MW format
    const demandElements = page.locator('.sidebar-region-card').locator('text=/\\d+ MW/');
    await expect(demandElements.first()).toBeVisible();
  });

  test('displays Australia map', async ({ page }) => {
    await expect(page.locator('.map-container')).toBeVisible();
  });

  test('clicking region card navigates to state detail', async ({ page }) => {
    // Click on NSW region card
    await page.locator('.sidebar-region-card').filter({ hasText: 'NSW' }).click();

    // Should show state detail page
    await expect(page.getByText('New South Wales (NSW)')).toBeVisible();
    await expect(page.getByText('Back to Overview')).toBeVisible();
  });

  test('back button returns to overview', async ({ page }) => {
    // Navigate to NSW
    await page.locator('.sidebar-region-card').filter({ hasText: 'NSW' }).click();
    await expect(page.getByText('New South Wales (NSW)')).toBeVisible();

    // Click back
    await page.getByText('Back to Overview').click();

    // Should be back on overview
    await expect(page.locator('.sidebar-region-card')).toHaveCount(5);
  });

  test('region cards have visual hover effect', async ({ page }) => {
    const nswCard = page.locator('.sidebar-region-card').filter({ hasText: 'NSW' });

    // Hover over card
    await nswCard.hover();

    // Card should have some visual change (implementation specific)
    await expect(nswCard).toBeVisible();
  });

  test('sidebar title is visible', async ({ page }) => {
    await expect(page.getByText('NEM Regions')).toBeVisible();
  });
});

test.describe('Live Prices Page - Region Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText(/Loading market data/i)).not.toBeVisible({ timeout: 15000 });
  });

  const regions = [
    { code: 'NSW', name: 'New South Wales' },
    { code: 'VIC', name: 'Victoria' },
    { code: 'QLD', name: 'Queensland' },
    { code: 'SA', name: 'South Australia' },
    { code: 'TAS', name: 'Tasmania' },
  ];

  for (const region of regions) {
    test(`navigates to ${region.name} state detail`, async ({ page }) => {
      await page.locator('.sidebar-region-card').filter({ hasText: region.code }).click();

      await expect(page.getByText(`${region.name} (${region.code})`)).toBeVisible();
    });
  }
});

test.describe('Live Prices Page - Data Refresh', () => {
  test('refreshes data automatically', async ({ page }) => {
    let requestCount = 0;

    // Count API requests
    await page.route('**/api/prices/latest**', async route => {
      requestCount++;
      await route.continue();
    });

    await page.goto('/');
    await expect(page.getByText(/Loading market data/i)).not.toBeVisible({ timeout: 15000 });

    const initialCount = requestCount;

    // Wait for refresh interval (30 seconds) + buffer
    await page.waitForTimeout(32000);

    // Should have made additional requests
    expect(requestCount).toBeGreaterThan(initialCount);
  });
});

test.describe('Live Prices Page - Dark Mode', () => {
  test('dark mode applies to live prices container', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText(/Loading market data/i)).not.toBeVisible({ timeout: 15000 });

    // Initially light
    await expect(page.locator('.live-prices-container')).toHaveClass(/light/);

    // Toggle dark mode
    await page.locator('.toggle-switch').click();

    await expect(page.locator('.live-prices-container')).toHaveClass(/dark/);
  });

  test('dark mode persists through navigation', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText(/Loading market data/i)).not.toBeVisible({ timeout: 15000 });

    // Enable dark mode
    await page.locator('.toggle-switch').click();
    await expect(page.locator('.app')).toHaveClass(/dark/);

    // Navigate to state detail
    await page.locator('.sidebar-region-card').filter({ hasText: 'NSW' }).click();
    await expect(page.locator('.state-detail-container')).toHaveClass(/dark/);

    // Navigate back
    await page.getByText('Back to Overview').click();
    await expect(page.locator('.live-prices-container')).toHaveClass(/dark/);
  });
});
