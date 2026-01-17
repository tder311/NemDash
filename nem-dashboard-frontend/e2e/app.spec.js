// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('App', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('loads and displays header', async ({ page }) => {
    await expect(page.locator('h1')).toContainText('NEM Market Dashboard');
  });

  test.skip('dark mode toggle is visible', async ({ page }) => {
    await expect(page.locator('.toggle-switch')).toBeVisible();
  });

  test('dark mode toggle works', async ({ page }) => {
    // Initially light mode
    await expect(page.locator('.app')).toHaveClass(/light/);

    // Click toggle
    await page.locator('.toggle-switch').click();

    // Should be dark mode
    await expect(page.locator('.app')).toHaveClass(/dark/);
  });

  test.skip('dark mode toggle label is visible', async ({ page }) => {
    // "Dark" label is always visible
    await expect(page.locator('.dark-mode-toggle')).toContainText('Dark');

    // Toggle switch should not have active class initially (light mode)
    await expect(page.locator('.toggle-switch')).not.toHaveClass(/active/);

    await page.locator('.toggle-switch').click();

    // Toggle switch should have active class in dark mode
    await expect(page.locator('.toggle-switch')).toHaveClass(/active/);
  });

  test.skip('body class updates with dark mode', async ({ page }) => {
    await expect(page.locator('body')).toHaveClass(/light/);

    await page.locator('.toggle-switch').click();

    await expect(page.locator('body')).toHaveClass(/dark/);
  });
});

test.describe('App - Loading States', () => {
  test.skip('shows loading indicator while fetching data', async ({ page }) => {
    // Slow down network to see loading state
    await page.route('**/api/**', async route => {
      await new Promise(resolve => setTimeout(resolve, 1000));
      await route.continue();
    });

    await page.goto('/');

    // Should show loading initially
    await expect(page.getByText(/Loading market data/i)).toBeVisible();

    // Eventually should complete
    await expect(page.getByText(/Loading market data/i)).not.toBeVisible({ timeout: 15000 });
  });
});

test.describe('App - Error Handling', () => {
  test('handles API errors gracefully', async ({ page }) => {
    // Mock API to return error
    await page.route('**/api/prices/**', route => {
      route.fulfill({
        status: 500,
        body: JSON.stringify({ error: 'Internal server error' })
      });
    });

    await page.goto('/');

    // App should still render (with fallback data)
    await expect(page.locator('.app')).toBeVisible();
  });
});
