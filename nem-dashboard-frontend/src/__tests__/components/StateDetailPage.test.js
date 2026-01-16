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
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Check all three API calls were made
    expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/prices/history?hours=24&price_type=PUBLIC');
    expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/generation/current');
    expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/summary');
  });

  test('displays region name and code', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

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
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getByText('Current Price')).toBeInTheDocument();
    });

    expect(screen.getByText('$92.30')).toBeInTheDocument();
    expect(screen.getByText('7500')).toBeInTheDocument();
    expect(screen.getByText('7200')).toBeInTheDocument();
    expect(screen.getByText('57')).toBeInTheDocument();
  });

  test('displays fuel breakdown table', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getByText('Generation by Fuel Source')).toBeInTheDocument();
    });

    // Check fuel sources are displayed
    expect(screen.getByText('Coal')).toBeInTheDocument();
    expect(screen.getByText('Solar')).toBeInTheDocument();
    expect(screen.getByText('Wind')).toBeInTheDocument();
    expect(screen.getByText('Gas')).toBeInTheDocument();
    expect(screen.getByText('Hydro')).toBeInTheDocument();
  });

  test('back button calls onBack prop', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getByText('Back to Overview')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Back to Overview'));

    expect(mockOnBack).toHaveBeenCalledTimes(1);
  });

  test('time range selector changes state', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      // Re-fetch after time range change
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    const select = screen.getByRole('combobox');
    expect(select).toHaveValue('24');

    // Change to 48 hours
    fireEvent.change(select, { target: { value: '48' } });

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/prices/history?hours=48&price_type=PUBLIC');
    });
  });

  test('renders Plotly charts', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getAllByTestId('plotly-chart').length).toBe(2);
    });
  });

  test('auto-refreshes every 60 seconds', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    expect(axios.get).toHaveBeenCalledTimes(3);

    // Advance timer by 60 seconds
    act(() => {
      jest.advanceTimersByTime(60000);
    });

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(6);
    });
  });

  test('falls back to sample data on API error', async () => {
    axios.get.mockRejectedValue(new Error('Network error'));

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Should display the page with fallback data
    expect(screen.getByText('New South Wales (NSW)')).toBeInTheDocument();
    expect(screen.getByText('$0.00')).toBeInTheDocument();
  });

  test('displays Last Updated timestamp', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getByText(/Last Updated/i)).toBeInTheDocument();
    });
  });

  test('applies dark mode class', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={true} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(document.querySelector('.state-detail-container')).toHaveClass('dark');
    });
  });

  test('applies light mode class when darkMode is false', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(document.querySelector('.state-detail-container')).toHaveClass('light');
    });
  });

  test('displays correct region for VIC', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: { ...mockSummary, region: 'VIC' } });

    render(<StateDetailPage region="VIC" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Region name and code are rendered together as "Victoria (VIC)"
    expect(screen.getByText(/Victoria \(VIC\)/i)).toBeInTheDocument();
    expect(axios.get).toHaveBeenCalledWith('/api/region/VIC/prices/history?hours=24&price_type=PUBLIC');
    expect(axios.get).toHaveBeenCalledWith('/api/region/VIC/generation/current');
    expect(axios.get).toHaveBeenCalledWith('/api/region/VIC/summary');
  });

  test('re-fetches data when region prop changes', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: { ...mockSummary, region: 'VIC' } });

    const { rerender } = render(
      <StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />
    );

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    expect(axios.get).toHaveBeenCalledTimes(3);

    // Change region
    rerender(<StateDetailPage region="VIC" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(6);
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
      .mockResolvedValueOnce({ data: summaryWithNulls });

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
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    expect(screen.getByText('Generation by Fuel Source')).toBeInTheDocument();
    // Table should exist (may have header or fallback data)
    expect(screen.getByRole('table')).toBeInTheDocument();
  });

  test('handles empty price history', async () => {
    axios.get
      .mockResolvedValueOnce({ data: { data: [] } })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    // Charts should still render (with empty data)
    expect(screen.getAllByTestId('plotly-chart').length).toBe(2);
  });

  test('clears interval on unmount', async () => {
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });

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
      .mockResolvedValueOnce({ data: mockSummary });
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

describe('StateDetailPage time range options', () => {
  const mockOnBack = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary });
  });

  test('has all time range options', async () => {
    render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

    await waitFor(() => {
      expect(screen.getByRole('combobox')).toBeInTheDocument();
    });

    const options = screen.getAllByRole('option');
    expect(options).toHaveLength(5);
    expect(options[0]).toHaveValue('6');
    expect(options[1]).toHaveValue('12');
    expect(options[2]).toHaveValue('24');
    expect(options[3]).toHaveValue('48');
    expect(options[4]).toHaveValue('168');
  });
});
