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

// Mock axios module - define everything inline to avoid hoisting issues
jest.mock('axios', () => {
  // Smart default mock implementation for axios.get
  const defaultGet = jest.fn((url) => {
    if (url && typeof url === 'string') {
      if (url.includes('/prices/history') || (url.includes('/region/') && url.includes('/prices/'))) {
        return Promise.resolve({ data: { data: [], count: 0, message: 'No data' } });
      }
      if (url.includes('/generation/current')) {
        return Promise.resolve({ data: { fuel_mix: [], total_generation: 0, region: 'NSW' } });
      }
      if (url.includes('/summary')) {
        return Promise.resolve({ data: { region: 'NSW', latest_price: 0, total_demand: 0, total_generation: 0, generator_count: 0 } });
      }
    }
    return Promise.resolve({ data: { data: [] } });
  });

  const defaultPost = jest.fn(() => Promise.resolve({ data: {} }));
  const defaultPut = jest.fn(() => Promise.resolve({ data: {} }));
  const defaultDelete = jest.fn(() => Promise.resolve({ data: {} }));

  return {
    __esModule: true,
    default: {
      get: defaultGet,
      post: defaultPost,
      put: defaultPut,
      delete: defaultDelete,
    },
    get: defaultGet,
    post: defaultPost,
    put: defaultPut,
    delete: defaultDelete,
  };
});

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
