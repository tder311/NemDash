import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import axios from 'axios';
import StateDetailPage from '../../components/StateDetailPage';

const mockPriceHistory = {
  data: [
    { settlementdate: '2025-01-15T08:00:00', price: 80.50 },
    { settlementdate: '2025-01-15T09:00:00', price: 85.20 },
    { settlementdate: '2025-01-15T10:00:00', price: 92.30 },
  ]
};

const mockFuelMix = {
  fuel_mix: [
    { fuel_source: 'Coal', generation_mw: 4500, percentage: 45.0, unit_count: 12 },
    { fuel_source: 'Solar', generation_mw: 2000, percentage: 20.0, unit_count: 50 },
    { fuel_source: 'Wind', generation_mw: 1500, percentage: 15.0, unit_count: 30 },
    { fuel_source: 'Gas', generation_mw: 1200, percentage: 12.0, unit_count: 8 },
    { fuel_source: 'Hydro', generation_mw: 800, percentage: 8.0, unit_count: 5 },
  ]
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
    { period: '2025-01-15T08:00:00', fuel_source: 'Coal', generation_mw: 4500 },
    { period: '2025-01-15T08:00:00', fuel_source: 'Solar', generation_mw: 2000 },
    { period: '2025-01-15T09:00:00', fuel_source: 'Coal', generation_mw: 4600 },
    { period: '2025-01-15T09:00:00', fuel_source: 'Solar', generation_mw: 2200 },
  ]
};

describe('StateDetailPage', () => {
  const mockOnBack = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('shows loading state initially', async () => {
    axios.get.mockImplementation(() => new Promise(() => {}));

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    expect(screen.getByText(/Loading New South Wales data/i)).toBeInTheDocument();
  });

  test('fetches all data in parallel on mount', async () => {
    const mockDataRange = { region: 'NSW', earliest_date: '2025-01-01', latest_date: '2026-01-27' };

    axios.get
      .mockResolvedValueOnce({ data: mockDataRange })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Check API calls were made (with date range params)
    expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/data-range');
    expect(axios.get).toHaveBeenCalledWith(expect.stringContaining('/api/region/NSW/prices/history'));
    expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/summary');
    expect(axios.get).toHaveBeenCalledWith(expect.stringContaining('/api/region/NSW/generation/history'));
  });

  test('displays region name and code', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Region name and code are rendered together as "New South Wales (NSW)"
    expect(screen.getByText(/New South Wales \(NSW\)/i)).toBeInTheDocument();
  });

  test('displays summary cards with correct values', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getByText('Current Price')).toBeInTheDocument();
    });

    expect(screen.getByText('$92.30')).toBeInTheDocument();
    expect(screen.getByText('7500')).toBeInTheDocument();
    expect(screen.getByText('7200')).toBeInTheDocument();
    expect(screen.getByText('57')).toBeInTheDocument();
  });

  test('displays generation history chart', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Check that 3 charts are rendered (price/demand, fuel mix, and generation history)
    expect(screen.getAllByTestId('plotly-chart').length).toBe(3);
  });

  test('back button calls onBack prop', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getByText('Back to Overview')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Back to Overview'));

    expect(mockOnBack).toHaveBeenCalledTimes(1);
  });

  test('date range selector updates API calls', async () => {
    const mockDataRange = { region: 'NSW', earliest_date: '2025-01-01', latest_date: '2026-01-27' };

    axios.get
      .mockResolvedValueOnce({ data: mockDataRange })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory })
      // Re-fetch after date range change
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Check that date dropdowns are rendered
    expect(screen.getByLabelText(/start day/i)).toBeInTheDocument();

    // Change start month
    const startMonthSelect = screen.getByLabelText(/start month/i);
    fireEvent.change(startMonthSelect, { target: { value: '6' } });

    await waitFor(() => {
      // Should have made new API calls with updated date range
      expect(axios.get).toHaveBeenCalledTimes(7); // Initial 4 + refetch 3
    });
  });

  test('renders Plotly charts', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getAllByTestId('plotly-chart').length).toBe(3);
    });
  });

  test('auto-refreshes every 60 seconds', async () => {
    const mockDataRange = { region: 'NSW', earliest_date: '2025-01-01', latest_date: '2026-01-27' };

    axios.get
      .mockResolvedValueOnce({ data: mockDataRange })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    expect(axios.get).toHaveBeenCalledTimes(4);

    // Advance timer by 60 seconds
    act(() => {
      jest.advanceTimersByTime(60000);
    });

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(7); // +3 for refresh (data-range only fetched once)
    });
  });

  test('falls back to sample data on API error', async () => {
    // Mock data-range to succeed but other calls to fail
    axios.get.mockImplementation((url) => {
      if (url.includes('/data-range')) {
        return Promise.resolve({
          data: {
            region: 'NSW',
            earliest_date: '2025-01-01',
            latest_date: '2026-01-27'
          }
        });
      }
      return Promise.reject(new Error('Network error'));
    });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    // Advance timers to allow promises to resolve
    jest.advanceTimersByTime(100);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    }, { timeout: 3000 });

    // Should display the page with fallback data
    expect(screen.getByText('New South Wales (NSW)')).toBeInTheDocument();
    expect(screen.getByText('$0.00')).toBeInTheDocument();
  });

  test('displays Last Updated timestamp', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getByText(/Last Updated/i)).toBeInTheDocument();
    });
  });

  test('applies dark mode class', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={true} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(document.querySelector('.state-detail-container')).toHaveClass('dark');
    });
  });

  test('applies light mode class when darkMode is false', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(document.querySelector('.state-detail-container')).toHaveClass('light');
    });
  });

  test('displays correct region for VIC', async () => {
    const mockDataRange = { region: 'VIC', earliest_date: '2025-01-01', latest_date: '2026-01-27' };

    axios.get
      .mockResolvedValueOnce({ data: mockDataRange })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: { ...mockSummary, region: 'VIC' } })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="VIC" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Region name and code are rendered together as "Victoria (VIC)"
    expect(screen.getByText(/Victoria \(VIC\)/i)).toBeInTheDocument();
    expect(axios.get).toHaveBeenCalledWith('/api/region/VIC/data-range');
    expect(axios.get).toHaveBeenCalledWith(expect.stringContaining('/api/region/VIC/prices/history'));
    expect(axios.get).toHaveBeenCalledWith('/api/region/VIC/summary');
    expect(axios.get).toHaveBeenCalledWith(expect.stringContaining('/api/region/VIC/generation/history'));
  });

  test('re-fetches data when region prop changes', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: { ...mockSummary, region: 'VIC' } })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    const { rerender } = render(
      <StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />
    );

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    expect(axios.get).toHaveBeenCalledTimes(4);

    // Change region
    rerender(<StateDetailPage region="VIC" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(8);
    });
  });

  test('handles null values in summary gracefully', async () => {
    const summaryWithNulls = {
      region: 'NSW',
      latest_price: null,
      total_demand: null,
      total_generation: null,
      generator_count: null
    };

    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: summaryWithNulls })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Should display with fallback values
    expect(screen.getByText('$0.00')).toBeInTheDocument();
  });

  test('handles empty fuel mix', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: { fuel_mix: [] } })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Charts should still render (with empty data)
    expect(screen.getAllByTestId('plotly-chart').length).toBe(3);
  });

  test('handles empty price history', async () => {
    axios.get
      .mockResolvedValueOnce({ data: { data: [] } })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Charts should still render (with empty data)
    expect(screen.getAllByTestId('plotly-chart').length).toBe(3);
  });

  test('clears interval on unmount', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    const { unmount } = render(
      <StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />
    );

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    unmount();

    // Advance time - should not cause errors
    act(() => {
      jest.advanceTimersByTime(60000);
    });
  });
});

describe('StateDetailPage REGION_NAMES mapping', () => {
  const mockOnBack = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });
  });

  test.each([
    ['NSW', 'New South Wales'],
    ['VIC', 'Victoria'],
    ['QLD', 'Queensland'],
    ['SA', 'South Australia'],
    ['TAS', 'Tasmania'],
  ])('displays full name for %s', async (code, fullName) => {
    render(<StateDetailPage region={code} darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getByText(new RegExp(fullName))).toBeInTheDocument();
    });
  });
});

describe('StateDetailPage Date Range Selector', () => {
  const mockOnBack = jest.fn();
  const mockDataRange = {
    region: 'NSW',
    earliest_date: '2025-01-01T00:00:00',
    latest_date: '2026-01-27T00:00:00',
    message: 'Data range for NSW'
  };

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('renders date dropdowns for start and end dates', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockDataRange })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Check for date dropdowns
    expect(screen.getByLabelText(/start day/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/start month/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/start year/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/end day/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/end month/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/end year/i)).toBeInTheDocument();
  });

  test('displays duration text based on selected range', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockDataRange })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Should show duration (e.g., "24 hours" or "7 days")
    expect(screen.getByText(/\d+ (hours?|days?|months?)/i)).toBeInTheDocument();
  });

  test('fetches data range on mount', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockDataRange })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/data-range');
    });
  });

  test('updates data when date range changes', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockDataRange })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory })
      // After date change
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Change start month
    const startMonthSelect = screen.getByLabelText(/start month/i);
    fireEvent.change(startMonthSelect, { target: { value: '6' } });

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledWith(expect.stringContaining('start_date'));
    });
  });
});

describe('StateDetailPage Aggregated Fuel Mix', () => {
  const mockOnBack = jest.fn();
  const mockDataRange = {
    region: 'NSW',
    earliest_date: '2025-01-01T00:00:00',
    latest_date: '2026-01-27T00:00:00',
    message: 'Data range for NSW'
  };

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('displays fuel mix chart with aggregated data', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockDataRange })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Should have chart for fuel mix
    expect(screen.getAllByTestId('plotly-chart').length).toBeGreaterThanOrEqual(3);
  });

  test('fuel mix chart uses aggregated generation data', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockDataRange })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Should render charts
    expect(screen.getAllByTestId('plotly-chart').length).toBe(3);
  });
});
