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

// Mock axios with default resolved values
jest.mock('axios', () => ({
  __esModule: true,
  default: {
    get: jest.fn(() => Promise.resolve({ data: { data: [] } })),
    post: jest.fn(() => Promise.resolve({ data: {} })),
    put: jest.fn(() => Promise.resolve({ data: {} })),
    delete: jest.fn(() => Promise.resolve({ data: {} })),
  },
  get: jest.fn(() => Promise.resolve({ data: { data: [] } })),
  post: jest.fn(() => Promise.resolve({ data: {} })),
  put: jest.fn(() => Promise.resolve({ data: {} })),
  delete: jest.fn(() => Promise.resolve({ data: {} })),
}));

// Get reference to the mocked axios for resetting
const axios = require('axios');

// Reset axios mocks before each test - restore default implementation after clearAllMocks
beforeEach(() => {
  // Only reset if the mock doesn't have an implementation set by the test
  if (!axios.get.getMockImplementation || !axios.get.getMockImplementation()) {
    axios.get.mockImplementation(() => Promise.resolve({ data: { data: [] } }));
  }
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
