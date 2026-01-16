// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom';

// Mock matchMedia for components that use it
window.matchMedia = window.matchMedia || function() {
  return {
    matches: false,
    addListener: function() {},
    removeListener: function() {}
  };
};

// Mock fetch for API calls - returns empty SVG by default
const mockFetch = jest.fn(() =>
  Promise.resolve({
    text: () => Promise.resolve('<svg></svg>'),
    ok: true,
  })
);
global.fetch = mockFetch;

// Reset fetch mock before each test
beforeEach(() => {
  global.fetch.mockClear();
  global.fetch.mockImplementation(() =>
    Promise.resolve({
      text: () => Promise.resolve('<svg></svg>'),
      ok: true,
    })
  );
});

// Smart default mock implementation for axios.get that handles all common API patterns
const defaultAxiosGetImplementation = (url) => {
  // Return appropriate empty/default data based on URL pattern
  if (url && typeof url === 'string') {
    if (url.includes('/prices/history') || url.includes('/region/') && url.includes('/prices/')) {
      return Promise.resolve({ data: { data: [], count: 0, message: 'No data' } });
    }
    if (url.includes('/generation/current')) {
      return Promise.resolve({ data: { fuel_mix: [], total_generation: 0, region: 'NSW' } });
    }
    if (url.includes('/summary')) {
      return Promise.resolve({ data: { region: 'NSW', latest_price: 0, total_demand: 0, total_generation: 0, generator_count: 0 } });
    }
  }
  // Default response for any other URL
  return Promise.resolve({ data: { data: [] } });
};

// Create mock functions for axios - these will be reused
const mockAxiosGet = jest.fn(defaultAxiosGetImplementation);
const mockAxiosPost = jest.fn(() => Promise.resolve({ data: {} }));
const mockAxiosPut = jest.fn(() => Promise.resolve({ data: {} }));
const mockAxiosDelete = jest.fn(() => Promise.resolve({ data: {} }));

// Mock axios module
jest.mock('axios', () => ({
  __esModule: true,
  default: {
    get: mockAxiosGet,
    post: mockAxiosPost,
    put: mockAxiosPut,
    delete: mockAxiosDelete,
  },
  get: mockAxiosGet,
  post: mockAxiosPost,
  put: mockAxiosPut,
  delete: mockAxiosDelete,
}));

// Note: Test files may call jest.clearAllMocks() in their beforeEach, but this only clears
// call history, not implementations. So the default implementation will remain active unless
// the test explicitly overrides it with mockResolvedValueOnce() or mockImplementation().

// Mock Plotly - it doesn't work well in test environment
jest.mock('react-plotly.js', () => {
  return function MockPlot(props) {
    return <div data-testid="plotly-chart" data-layout={JSON.stringify(props.layout)} />;
  };
});

// Suppress console errors/warnings in tests (optional)
// Uncomment if you want cleaner test output
// const originalError = console.error;
// beforeAll(() => {
//   console.error = (...args) => {
//     if (typeof args[0] === 'string' && args[0].includes('Warning:')) {
//       return;
//     }
//     originalError.call(console, ...args);
//   };
// });
// afterAll(() => {
//   console.error = originalError;
// });
