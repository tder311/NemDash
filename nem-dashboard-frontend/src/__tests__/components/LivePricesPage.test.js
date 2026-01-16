import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import axios from 'axios';
import LivePricesPage from '../../components/LivePricesPage';

// Mock child components
jest.mock('../../components/RegionSidebar', () => {
  return function MockRegionSidebar({ regions, onRegionHover, onRegionLeave, onRegionClick }) {
    return (
      <div data-testid="region-sidebar">
        {regions.map(r => (
          <div
            key={r.region}
            data-testid={`sidebar-region-${r.region}`}
            onClick={() => onRegionClick(r.region)}
            onMouseEnter={() => onRegionHover(r.region)}
            onMouseLeave={() => onRegionLeave()}
          >
            {r.region}: ${r.price}
          </div>
        ))}
      </div>
    );
  };
});

jest.mock('../../components/AustraliaMap', () => {
  return function MockAustraliaMap({ hoveredRegion, onRegionClick }) {
    return (
      <div data-testid="australia-map" data-hovered={hoveredRegion}>
        <button data-testid="map-nsw" onClick={() => onRegionClick('NSW')}>NSW</button>
        <button data-testid="map-vic" onClick={() => onRegionClick('VIC')}>VIC</button>
      </div>
    );
  };
});

jest.mock('../../components/StateDetailPage', () => {
  return function MockStateDetailPage({ region, onBack }) {
    return (
      <div data-testid="state-detail-page">
        <span data-testid="detail-region">{region}</span>
        <button data-testid="back-button" onClick={onBack}>Back</button>
      </div>
    );
  };
});

const mockTradingData = {
  data: [
    { region: 'NSW', price: 85.50, settlementdate: '2025-01-15T10:30:00' },
    { region: 'VIC', price: 72.30, settlementdate: '2025-01-15T10:30:00' },
    { region: 'QLD', price: 65.10, settlementdate: '2025-01-15T10:30:00' },
    { region: 'SA', price: 95.20, settlementdate: '2025-01-15T10:30:00' },
    { region: 'TAS', price: 55.00, settlementdate: '2025-01-15T10:30:00' },
  ]
};

const mockDispatchData = {
  data: [
    { region: 'NSW', totaldemand: 7500 },
    { region: 'VIC', totaldemand: 5200 },
    { region: 'QLD', totaldemand: 6800 },
    { region: 'SA', totaldemand: 2100 },
    { region: 'TAS', totaldemand: 1200 },
  ]
};

describe('LivePricesPage', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('shows loading state initially', async () => {
    // Don't resolve the promise yet
    axios.get.mockImplementation(() => new Promise(() => {}));

    render(<LivePricesPage darkMode={false} />);

    expect(screen.getByText(/Loading market data/i)).toBeInTheDocument();
    expect(screen.getByClassName ? screen.getByClassName('spinner') : document.querySelector('.spinner')).toBeInTheDocument();
  });

  test('fetches and displays data on mount', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)  // trading prices
      .mockResolvedValueOnce(mockDispatchData); // dispatch data

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading market data/i)).not.toBeInTheDocument();
    });

    expect(axios.get).toHaveBeenCalledWith('/api/prices/latest?price_type=TRADING');
    expect(axios.get).toHaveBeenCalledWith('/api/prices/latest?price_type=DISPATCH');
  });

  test('merges trading prices with dispatch demand data', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData);

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByTestId('region-sidebar')).toBeInTheDocument();
    });

    // Check that NSW shows the combined data
    expect(screen.getByTestId('sidebar-region-NSW')).toHaveTextContent('NSW: $85.5');
  });

  test('displays Last Updated timestamp', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData);

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByText(/Last Updated/i)).toBeInTheDocument();
    });
  });

  test('falls back to sample data on API error', async () => {
    axios.get.mockRejectedValue(new Error('Network error'));

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading market data/i)).not.toBeInTheDocument();
    });

    // Should still render with fallback data
    expect(screen.getByTestId('region-sidebar')).toBeInTheDocument();
  });

  test('polls for data every 30 seconds', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData)
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData);

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByTestId('region-sidebar')).toBeInTheDocument();
    });

    // Initial fetch
    expect(axios.get).toHaveBeenCalledTimes(2);

    // Advance timer by 30 seconds
    act(() => {
      jest.advanceTimersByTime(30000);
    });

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(4);
    });
  });

  test('clears interval on unmount', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData);

    const { unmount } = render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByTestId('region-sidebar')).toBeInTheDocument();
    });

    unmount();

    // Should not cause any errors after unmount
    act(() => {
      jest.advanceTimersByTime(30000);
    });
  });

  test('handles region hover events', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData);

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByTestId('region-sidebar')).toBeInTheDocument();
    });

    // Hover over NSW
    fireEvent.mouseEnter(screen.getByTestId('sidebar-region-NSW'));

    // Check that the map receives the hovered region
    expect(screen.getByTestId('australia-map')).toHaveAttribute('data-hovered', 'NSW');

    // Leave
    fireEvent.mouseLeave(screen.getByTestId('sidebar-region-NSW'));
    expect(screen.getByTestId('australia-map')).toHaveAttribute('data-hovered', '');
  });

  test('navigates to StateDetailPage when region is clicked', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData);

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByTestId('region-sidebar')).toBeInTheDocument();
    });

    // Click on NSW
    fireEvent.click(screen.getByTestId('sidebar-region-NSW'));

    // StateDetailPage should be shown
    await waitFor(() => {
      expect(screen.getByTestId('state-detail-page')).toBeInTheDocument();
    });

    expect(screen.getByTestId('detail-region')).toHaveTextContent('NSW');
  });

  test('returns to overview when back is clicked', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData);

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByTestId('region-sidebar')).toBeInTheDocument();
    });

    // Click on VIC
    fireEvent.click(screen.getByTestId('sidebar-region-VIC'));

    await waitFor(() => {
      expect(screen.getByTestId('state-detail-page')).toBeInTheDocument();
    });

    // Click back button
    fireEvent.click(screen.getByTestId('back-button'));

    await waitFor(() => {
      expect(screen.getByTestId('region-sidebar')).toBeInTheDocument();
    });

    expect(screen.queryByTestId('state-detail-page')).not.toBeInTheDocument();
  });

  test('clicking map region navigates to StateDetailPage', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData);

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByTestId('australia-map')).toBeInTheDocument();
    });

    // Click on map NSW button
    fireEvent.click(screen.getByTestId('map-nsw'));

    await waitFor(() => {
      expect(screen.getByTestId('state-detail-page')).toBeInTheDocument();
    });

    expect(screen.getByTestId('detail-region')).toHaveTextContent('NSW');
  });

  test('applies dark mode class', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData);

    render(<LivePricesPage darkMode={true} />);

    await waitFor(() => {
      expect(document.querySelector('.live-prices-container')).toHaveClass('dark');
    });
  });

  test('applies light mode class when darkMode is false', async () => {
    axios.get
      .mockResolvedValueOnce(mockTradingData)
      .mockResolvedValueOnce(mockDispatchData);

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(document.querySelector('.live-prices-container')).toHaveClass('light');
    });
  });

  test('handles empty API response gracefully', async () => {
    axios.get
      .mockResolvedValueOnce({ data: [] })
      .mockResolvedValueOnce({ data: [] });

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading market data/i)).not.toBeInTheDocument();
    });

    expect(screen.getByTestId('region-sidebar')).toBeInTheDocument();
  });

  test('handles missing totaldemand in dispatch data', async () => {
    const tradingDataOnly = {
      data: [
        { region: 'NSW', price: 85.50, settlementdate: '2025-01-15T10:30:00' },
      ]
    };

    axios.get
      .mockResolvedValueOnce(tradingDataOnly)
      .mockResolvedValueOnce({ data: [] }); // No dispatch data

    render(<LivePricesPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByTestId('region-sidebar')).toBeInTheDocument();
    });

    // Should still render, with 0 for missing demand
    expect(screen.getByTestId('sidebar-region-NSW')).toBeInTheDocument();
  });
});
