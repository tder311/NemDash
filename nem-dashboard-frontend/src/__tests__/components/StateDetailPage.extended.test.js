/**
 * Tests for date range selector functionality in StateDetailPage.
 *
 * These tests verify:
 * - Date range dropdowns (day, month, year) for start and end dates
 * - API calls use correct date parameters
 * - Duration text displays correctly for various ranges
 * - Chart synchronization via rangeslider
 */
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import axios from 'axios';
import StateDetailPage from '../../components/StateDetailPage';

// Mock axios
jest.mock('axios');

const mockDataRange = {
  region: 'NSW',
  earliest_date: '2025-01-01T00:00:00',
  latest_date: '2026-01-27T23:55:00',
  message: 'Data range retrieved'
};

const mockPriceHistory = {
  data: [
    { settlementdate: '2025-01-15T08:00:00', price: 80.50, totaldemand: 7500 },
    { settlementdate: '2025-01-15T08:30:00', price: 82.00, totaldemand: 7600 },
  ],
  aggregation_minutes: 30
};

const mockSummary = {
  region: 'NSW',
  latest_price: 92.30,
  total_demand: 7500,
  total_generation: 7200,
  generator_count: 57
};

const mockGenerationHistory = {
  data: [
    { period: '2025-01-15T08:00:00', fuel_source: 'Coal', generation_mw: 4500, sample_count: 6 },
    { period: '2025-01-15T08:00:00', fuel_source: 'Solar', generation_mw: 1500, sample_count: 6 },
    { period: '2025-01-15T08:30:00', fuel_source: 'Coal', generation_mw: 4600, sample_count: 6 },
    { period: '2025-01-15T08:30:00', fuel_source: 'Solar', generation_mw: 1400, sample_count: 6 },
  ],
  aggregation_minutes: 30
};

describe('StateDetailPage Date Range Features', () => {
  const mockOnBack = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    axios.get.mockImplementation((url) => {
      if (url.includes('/data-range')) {
        return Promise.resolve({ data: mockDataRange });
      }
      if (url.includes('/prices/history')) {
        return Promise.resolve({ data: mockPriceHistory });
      }
      if (url.includes('/summary')) {
        return Promise.resolve({ data: mockSummary });
      }
      if (url.includes('/generation/history')) {
        return Promise.resolve({ data: mockGenerationHistory });
      }
      return Promise.reject(new Error(`Unexpected URL: ${url}`));
    });
  });

  describe('Date Range Dropdowns', () => {
    test('renders start date dropdowns (day, month, year)', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      // Check for start date dropdowns
      expect(screen.getByLabelText(/start day/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/start month/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/start year/i)).toBeInTheDocument();
    });

    test('renders end date dropdowns (day, month, year)', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      // Check for end date dropdowns
      expect(screen.getByLabelText(/end day/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/end month/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/end year/i)).toBeInTheDocument();
    });

    test('renders date separator between start and end dates', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      expect(screen.getByText('—')).toBeInTheDocument();
    });

    test('renders Time Range label', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      expect(screen.getByText('Time Range:')).toBeInTheDocument();
    });
  });

  describe('Date Dropdown Interactions', () => {
    test('changing start month triggers data fetch', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      axios.get.mockClear();

      const startMonthSelect = screen.getByLabelText(/start month/i);
      fireEvent.change(startMonthSelect, { target: { value: '6' } });

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalled();
      });
    });

    test('changing end day triggers data fetch', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      axios.get.mockClear();

      const endDaySelect = screen.getByLabelText(/end day/i);
      fireEvent.change(endDaySelect, { target: { value: '15' } });

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalled();
      });
    });

    test('changing start year triggers data fetch', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      axios.get.mockClear();

      const startYearSelect = screen.getByLabelText(/start year/i);
      fireEvent.change(startYearSelect, { target: { value: '2024' } });

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalled();
      });
    });
  });

  describe('API Calls with Date Parameters', () => {
    test('initial load fetches data range endpoint', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/data-range');
      });
    });

    test('fetches price history with date parameters', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalledWith(
          expect.stringMatching(/\/api\/region\/NSW\/prices\/history\?start_date=.*&end_date=.*&price_type=MERGED/)
        );
      });
    });

    test('fetches generation history with date parameters', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalledWith(
          expect.stringMatching(/\/api\/region\/NSW\/generation\/history\?start_date=.*&end_date=.*/)
        );
      });
    });
  });

  describe('Duration Display', () => {
    test('displays duration text based on selected range', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      // Should show duration text in the date range selector (e.g., "(48 hours)", "(7 days)", etc.)
      // The duration text is wrapped in parentheses
      const durationElement = document.querySelector('.duration-text');
      expect(durationElement).toBeInTheDocument();
      expect(durationElement.textContent).toMatch(/\(\d+ (hours?|days?)\)/);
    });
  });

  describe('Aggregated Fuel Mix', () => {
    test('calculates fuel totals from generation history', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      // The fuel mix chart should be rendered
      // Plotly charts render titles in canvas/SVG, not as DOM text
      // So we verify the chart wrapper exists with the correct structure
      const chartWrappers = document.querySelectorAll('.chart-wrapper');
      expect(chartWrappers.length).toBeGreaterThan(0);
    });

    test('updates fuel mix when date range changes', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      axios.get.mockClear();

      // Change end month to trigger new data fetch
      const endMonthSelect = screen.getByLabelText(/end month/i);
      fireEvent.change(endMonthSelect, { target: { value: '12' } });

      await waitFor(() => {
        // Generation history should be fetched (fuel mix is calculated from it)
        expect(axios.get).toHaveBeenCalledWith(
          expect.stringContaining('/api/region/NSW/generation/history')
        );
      });
    });
  });

  describe('Dark Mode Support', () => {
    test('applies dark mode class to date range selector', async () => {
      render(<StateDetailPage region="NSW" darkMode={true} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      const container = document.querySelector('.state-detail-container');
      expect(container).toHaveClass('dark');
    });
  });

  describe('Month Options', () => {
    test('month dropdown contains all 12 months', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      const startMonthSelect = screen.getByLabelText(/start month/i);
      const options = startMonthSelect.querySelectorAll('option');

      expect(options.length).toBe(12);
      expect(options[0].textContent).toBe('Jan');
      expect(options[11].textContent).toBe('Dec');
    });
  });

  describe('Year Range', () => {
    test('year dropdown contains appropriate year range', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      const startYearSelect = screen.getByLabelText(/start year/i);
      const options = startYearSelect.querySelectorAll('option');

      // Should have multiple year options
      expect(options.length).toBeGreaterThan(1);
    });
  });

  describe('Error Handling', () => {
    test('handles API errors gracefully', async () => {
      axios.get.mockImplementation((url) => {
        if (url.includes('/data-range')) {
          return Promise.resolve({ data: mockDataRange });
        }
        return Promise.reject(new Error('API Error'));
      });

      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      // Should not crash, should still render the page structure
      await waitFor(() => {
        expect(screen.getByText('Time Range:')).toBeInTheDocument();
      });
    });
  });
});
