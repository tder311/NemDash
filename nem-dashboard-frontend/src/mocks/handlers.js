/**
 * Mock data for frontend tests
 */

export const mockPriceData = {
  data: [
    { region: 'NSW', price: 85.50, totaldemand: 7500, settlementdate: '2025-01-15T10:30:00' },
    { region: 'VIC', price: 72.30, totaldemand: 5200, settlementdate: '2025-01-15T10:30:00' },
    { region: 'QLD', price: 65.10, totaldemand: 6800, settlementdate: '2025-01-15T10:30:00' },
    { region: 'SA', price: 95.20, totaldemand: 2100, settlementdate: '2025-01-15T10:30:00' },
    { region: 'TAS', price: 55.00, totaldemand: 1200, settlementdate: '2025-01-15T10:30:00' },
  ],
  count: 5,
  message: 'Retrieved 5 price records'
};

export const mockTradingPriceData = {
  data: [
    { region: 'NSW', price: 85.50, settlementdate: '2025-01-15T10:30:00' },
    { region: 'VIC', price: 72.30, settlementdate: '2025-01-15T10:30:00' },
    { region: 'QLD', price: 65.10, settlementdate: '2025-01-15T10:30:00' },
    { region: 'SA', price: 95.20, settlementdate: '2025-01-15T10:30:00' },
    { region: 'TAS', price: 55.00, settlementdate: '2025-01-15T10:30:00' },
  ],
  count: 5,
  message: 'Retrieved 5 trading price records'
};

export const mockDispatchPriceData = {
  data: [
    { region: 'NSW', price: 84.00, totaldemand: 7500, settlementdate: '2025-01-15T10:30:00' },
    { region: 'VIC', price: 71.00, totaldemand: 5200, settlementdate: '2025-01-15T10:30:00' },
    { region: 'QLD', price: 64.00, totaldemand: 6800, settlementdate: '2025-01-15T10:30:00' },
    { region: 'SA', price: 94.00, totaldemand: 2100, settlementdate: '2025-01-15T10:30:00' },
    { region: 'TAS', price: 54.00, totaldemand: 1200, settlementdate: '2025-01-15T10:30:00' },
  ],
  count: 5,
  message: 'Retrieved 5 dispatch price records'
};

export const mockFuelMix = {
  region: 'NSW',
  total_generation: 10000,
  fuel_mix: [
    { fuel_source: 'Coal', generation_mw: 4500, percentage: 45.0, unit_count: 12 },
    { fuel_source: 'Gas', generation_mw: 2000, percentage: 20.0, unit_count: 8 },
    { fuel_source: 'Solar', generation_mw: 1500, percentage: 15.0, unit_count: 45 },
    { fuel_source: 'Wind', generation_mw: 1200, percentage: 12.0, unit_count: 22 },
    { fuel_source: 'Hydro', generation_mw: 800, percentage: 8.0, unit_count: 5 },
  ],
  message: 'Retrieved fuel mix for NSW'
};

export const mockRegionSummary = {
  region: 'NSW',
  latest_price: 85.50,
  total_demand: 7500,
  total_generation: 7200,
  generator_count: 92,
  price_timestamp: '2025-01-15T10:30:00',
  message: 'Retrieved summary for NSW'
};

export const mockPriceHistory = {
  region: 'NSW',
  data: [
    { settlementdate: '2025-01-15T06:00:00', price: 65.00 },
    { settlementdate: '2025-01-15T07:00:00', price: 72.00 },
    { settlementdate: '2025-01-15T08:00:00', price: 85.00 },
    { settlementdate: '2025-01-15T09:00:00', price: 92.00 },
    { settlementdate: '2025-01-15T10:00:00', price: 88.00 },
    { settlementdate: '2025-01-15T10:30:00', price: 85.50 },
  ],
  count: 6,
  hours: 24,
  price_type: 'PUBLIC',
  message: 'Retrieved 6 price records for NSW'
};

// Helper to create axios mock responses
export const createAxiosMockResponse = (data, status = 200) => ({
  data,
  status,
  statusText: 'OK',
  headers: {},
  config: {},
});

// Setup axios mocks for common scenarios
export const setupAxiosMocks = (axios) => {
  axios.get.mockImplementation((url) => {
    if (url.includes('/api/prices/latest') && url.includes('TRADING')) {
      return Promise.resolve(createAxiosMockResponse(mockTradingPriceData));
    }
    if (url.includes('/api/prices/latest') && url.includes('DISPATCH')) {
      return Promise.resolve(createAxiosMockResponse(mockDispatchPriceData));
    }
    if (url.includes('/api/prices/latest')) {
      return Promise.resolve(createAxiosMockResponse(mockPriceData));
    }
    if (url.includes('/api/region/') && url.includes('/generation/current')) {
      return Promise.resolve(createAxiosMockResponse(mockFuelMix));
    }
    if (url.includes('/api/region/') && url.includes('/summary')) {
      return Promise.resolve(createAxiosMockResponse(mockRegionSummary));
    }
    if (url.includes('/api/region/') && url.includes('/prices/history')) {
      return Promise.resolve(createAxiosMockResponse(mockPriceHistory));
    }
    if (url.includes('/api/prices/history')) {
      return Promise.resolve(createAxiosMockResponse({ data: [], count: 0, message: 'No data' }));
    }
    // Return empty data for unmatched URLs instead of rejecting
    return Promise.resolve(createAxiosMockResponse({ data: [] }));
  });
};

// Reset all mocks
export const resetAxiosMocks = (axios) => {
  axios.get.mockReset();
  axios.post.mockReset();
};
