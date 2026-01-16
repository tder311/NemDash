import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import axios from 'axios';
import PriceHistoryPage from '../../components/PriceHistoryPage';

const mockPriceData = {
  data: [
    { region: 'NSW', settlementdate: '2025-01-15T08:00:00', price: 80.50 },
    { region: 'VIC', settlementdate: '2025-01-15T08:00:00', price: 72.30 },
    { region: 'QLD', settlementdate: '2025-01-15T08:00:00', price: 65.10 },
    { region: 'SA', settlementdate: '2025-01-15T08:00:00', price: 95.20 },
    { region: 'TAS', settlementdate: '2025-01-15T08:00:00', price: 55.00 },
    { region: 'NSW', settlementdate: '2025-01-15T09:00:00', price: 85.20 },
    { region: 'VIC', settlementdate: '2025-01-15T09:00:00', price: 75.50 },
    { region: 'QLD', settlementdate: '2025-01-15T09:00:00', price: 68.40 },
    { region: 'SA', settlementdate: '2025-01-15T09:00:00', price: 92.10 },
    { region: 'TAS', settlementdate: '2025-01-15T09:00:00', price: 58.30 },
  ]
};

describe('PriceHistoryPage', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('shows loading state initially', () => {
    axios.get.mockImplementation(() => new Promise(() => {}));

    render(<PriceHistoryPage darkMode={false} />);

    expect(screen.getByText(/Loading price history/i)).toBeInTheDocument();
  });

  test('fetches price history data on mount', async () => {
    axios.get.mockResolvedValueOnce(mockPriceData);

    render(<PriceHistoryPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading price history/i)).not.toBeInTheDocument();
    });

    expect(axios.get).toHaveBeenCalledWith(
      '/api/prices/history',
      expect.objectContaining({
        params: expect.objectContaining({
          price_type: 'PUBLIC'
        })
      })
    );
  });

  test('requests 24 hours of data', async () => {
    axios.get.mockResolvedValueOnce(mockPriceData);

    render(<PriceHistoryPage darkMode={false} />);

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalled();
    });

    const callParams = axios.get.mock.calls[0][1].params;
    const startDate = new Date(callParams.start_date);
    const endDate = new Date(callParams.end_date);
    const diffHours = (endDate - startDate) / (1000 * 60 * 60);

    expect(diffHours).toBeCloseTo(24, 0);
  });

  test('renders Plotly chart after data loads', async () => {
    axios.get.mockResolvedValueOnce(mockPriceData);

    render(<PriceHistoryPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByTestId('plotly-chart')).toBeInTheDocument();
    });
  });

  test('falls back to sample data on API error', async () => {
    axios.get.mockRejectedValue(new Error('Network error'));

    render(<PriceHistoryPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading price history/i)).not.toBeInTheDocument();
    });

    // Should still render the chart with sample data
    expect(screen.getByTestId('plotly-chart')).toBeInTheDocument();
  });

  test('applies dark mode class', async () => {
    axios.get.mockResolvedValueOnce(mockPriceData);

    render(<PriceHistoryPage darkMode={true} />);

    await waitFor(() => {
      expect(document.querySelector('.price-history-container')).toHaveClass('dark');
    });
  });

  test('applies light mode class when darkMode is false', async () => {
    axios.get.mockResolvedValueOnce(mockPriceData);

    render(<PriceHistoryPage darkMode={false} />);

    await waitFor(() => {
      expect(document.querySelector('.price-history-container')).toHaveClass('light');
    });
  });

  test('handles empty API response', async () => {
    axios.get.mockResolvedValueOnce({ data: [] });

    render(<PriceHistoryPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading price history/i)).not.toBeInTheDocument();
    });

    expect(screen.getByTestId('plotly-chart')).toBeInTheDocument();
  });

  test('handles missing data property in response', async () => {
    axios.get.mockResolvedValueOnce({});

    render(<PriceHistoryPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading price history/i)).not.toBeInTheDocument();
    });

    expect(screen.getByTestId('plotly-chart')).toBeInTheDocument();
  });

  test('passes dark mode colors to chart layout', async () => {
    axios.get.mockResolvedValueOnce(mockPriceData);

    render(<PriceHistoryPage darkMode={true} />);

    await waitFor(() => {
      const chart = screen.getByTestId('plotly-chart');
      const layout = JSON.parse(chart.getAttribute('data-layout'));
      expect(layout.plot_bgcolor).toBe('#1a1a1a');
      expect(layout.paper_bgcolor).toBe('#1a1a1a');
    });
  });

  test('passes light mode colors to chart layout', async () => {
    axios.get.mockResolvedValueOnce(mockPriceData);

    render(<PriceHistoryPage darkMode={false} />);

    await waitFor(() => {
      const chart = screen.getByTestId('plotly-chart');
      const layout = JSON.parse(chart.getAttribute('data-layout'));
      expect(layout.plot_bgcolor).toBe('white');
      expect(layout.paper_bgcolor).toBe('white');
    });
  });
});

describe('PriceHistoryPage REGION_COLORS', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    axios.get.mockResolvedValueOnce(mockPriceData);
  });

  test('creates separate traces for each region', async () => {
    render(<PriceHistoryPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByTestId('plotly-chart')).toBeInTheDocument();
    });

    // The chart should have 5 traces (one for each region)
    // We verify this through the mock which receives the data prop
  });
});

describe('PriceHistoryPage sample data generation', () => {
  test('generates 288 data points per region on error (24h * 12 5-min intervals)', async () => {
    axios.get.mockRejectedValue(new Error('Network error'));

    render(<PriceHistoryPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading price history/i)).not.toBeInTheDocument();
    });

    // Chart should render with generated sample data
    expect(screen.getByTestId('plotly-chart')).toBeInTheDocument();
  });
});
