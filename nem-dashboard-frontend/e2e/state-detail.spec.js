// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('State Detail Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText(/Loading market data/i)).not.toBeVisible({ timeout: 15000 });
    // Navigate to NSW state detail
    await page.locator('.sidebar-region-card').filter({ hasText: 'NSW' }).click();
    // Wait for state detail to load
    await expect(page.getByText(/Loading New South Wales data/i)).not.toBeVisible({ timeout: 15000 });
  });

  test('displays state name and code', async ({ page }) => {
    await expect(page.getByText('New South Wales (NSW)')).toBeVisible();
  });

  test('displays back button', async ({ page }) => {
    await expect(page.getByText('Back to Overview')).toBeVisible();
  });

  test('displays Last Updated timestamp', async ({ page }) => {
    await expect(page.getByText(/Last Updated/i)).toBeVisible();
  });

  test('displays summary cards', async ({ page }) => {
    await expect(page.getByText('Current Price')).toBeVisible();
    await expect(page.getByText('Total Demand')).toBeVisible();
    await expect(page.getByText('Total Generation')).toBeVisible();
    await expect(page.getByText('Active Generators')).toBeVisible();
  });

  test('displays time range selector', async ({ page }) => {
    // Check for the time range selector container and label text
    await expect(page.locator('.time-range-selector')).toBeVisible();
    await expect(page.getByText('Time Range:')).toBeVisible();

    const select = page.getByRole('combobox');
    await expect(select).toHaveValue('24');
  });

  test('time range selector has all options', async ({ page }) => {
    const select = page.getByRole('combobox');

    await expect(select.locator('option[value="6"]')).toHaveText('6 Hours');
    await expect(select.locator('option[value="12"]')).toHaveText('12 Hours');
    await expect(select.locator('option[value="24"]')).toHaveText('24 Hours');
    await expect(select.locator('option[value="48"]')).toHaveText('48 Hours');
    await expect(select.locator('option[value="168"]')).toHaveText('7 Days');
  });

  test('changing time range updates chart', async ({ page }) => {
    let lastHoursParam = '';

    // Monitor API calls
    await page.route('**/api/region/NSW/prices/history**', async route => {
      const url = new URL(route.request().url());
      lastHoursParam = url.searchParams.get('hours') || '';
      await route.continue();
    });

    // Change to 48 hours
    await page.getByRole('combobox').selectOption('48');

    // Wait for the API call
    await page.waitForTimeout(1000);

    expect(lastHoursParam).toBe('48');
  });

  test('displays price history chart', async ({ page }) => {
    await expect(page.locator('.price-chart')).toBeVisible();
  });

  test('displays fuel mix chart', async ({ page }) => {
    await expect(page.locator('.fuel-chart')).toBeVisible();
  });

  test('displays fuel breakdown table', async ({ page }) => {
    await expect(page.getByText('Generation by Fuel Source')).toBeVisible();
    await expect(page.locator('.fuel-breakdown-table table')).toBeVisible();
  });

  test('fuel breakdown table has correct headers', async ({ page }) => {
    const headers = page.locator('.fuel-breakdown-table th');
    await expect(headers.nth(0)).toHaveText('Fuel Source');
    await expect(headers.nth(1)).toHaveText('Generation (MW)');
    await expect(headers.nth(2)).toHaveText('Share (%)');
    await expect(headers.nth(3)).toHaveText('Units');
  });

  test('back button navigates to overview', async ({ page }) => {
    await page.getByText('Back to Overview').click();

    await expect(page.locator('.sidebar-region-card')).toHaveCount(5);
    await expect(page.getByText('New South Wales (NSW)')).not.toBeVisible();
  });
});

test.describe('State Detail Page - All Regions', () => {
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
    test(`${region.name} detail page loads correctly`, async ({ page }) => {
      await page.locator('.sidebar-region-card').filter({ hasText: region.code }).click();

      await expect(page.getByText(`${region.name} (${region.code})`)).toBeVisible({ timeout: 15000 });
      await expect(page.getByText('Current Price')).toBeVisible();
      await expect(page.getByText('Generation by Fuel Source')).toBeVisible();
    });
  }
});

test.describe('State Detail Page - Dark Mode', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText(/Loading market data/i)).not.toBeVisible({ timeout: 15000 });
  });

  test('dark mode applies to state detail container', async ({ page }) => {
    // Enable dark mode first
    await page.locator('.toggle-switch').click();
    await expect(page.locator('.app')).toHaveClass(/dark/);

    // Navigate to state detail
    await page.locator('.sidebar-region-card').filter({ hasText: 'NSW' }).click();
    await expect(page.getByText(/Loading New South Wales data/i)).not.toBeVisible({ timeout: 15000 });

    await expect(page.locator('.state-detail-container')).toHaveClass(/dark/);
  });
});

test.describe('State Detail Page - Data Refresh', () => {
  test('auto-refreshes every 60 seconds', async ({ page }) => {
    // This test needs longer timeout since it waits for auto-refresh
    test.setTimeout(90000);

    let requestCount = 0;

    await page.route('**/api/region/NSW/**', async route => {
      requestCount++;
      await route.continue();
    });

    await page.goto('/');
    await expect(page.getByText(/Loading market data/i)).not.toBeVisible({ timeout: 15000 });

    await page.locator('.sidebar-region-card').filter({ hasText: 'NSW' }).click();
    await expect(page.getByText(/Loading New South Wales data/i)).not.toBeVisible({ timeout: 15000 });

    const initialCount = requestCount;

    // Wait for refresh interval (60 seconds) + buffer
    await page.waitForTimeout(62000);

    expect(requestCount).toBeGreaterThan(initialCount);
  });
});

test.describe('State Detail Page - Error Handling', () => {
  test('handles API errors gracefully', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText(/Loading market data/i)).not.toBeVisible({ timeout: 15000 });

    // Mock errors for state detail APIs
    await page.route('**/api/region/NSW/**', route => {
      route.fulfill({
        status: 500,
        body: JSON.stringify({ error: 'Internal server error' })
      });
    });

    await page.locator('.sidebar-region-card').filter({ hasText: 'NSW' }).click();

    // Should still render with fallback data
    await expect(page.getByText('New South Wales (NSW)')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('$0.00')).toBeVisible();
  });
});
