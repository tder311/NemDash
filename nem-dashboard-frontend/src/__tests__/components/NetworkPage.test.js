import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import axios from 'axios';
import NetworkPage from '../../components/NetworkPage';

const mockInterconnectors = {
  run_datetime: '2025-01-15T10:00:00+10:00',
  data: {
    'N-Q-MNSP1': [
      { interval_datetime: '2025-01-15T10:30:00+10:00', mwflow: 150, exportlimit: 210, importlimit: -100, marginalvalue: 0 },
      { interval_datetime: '2025-01-15T11:00:00+10:00', mwflow: 209, exportlimit: 210, importlimit: -100, marginalvalue: 12.5 },
    ],
    'VIC1-NSW1': [
      { interval_datetime: '2025-01-15T10:30:00+10:00', mwflow: -50, exportlimit: 600, importlimit: -600, marginalvalue: 0 },
    ],
  },
};

const mockConstraints = {
  run_datetime: '2025-01-15T10:00:00+10:00',
  constraints: [
    {
      constraintid: 'N>NIL_94T',
      category: 'network',
      regions: ['NSW1'],
      kind: 'thermal',
      label: 'NSW · thermal',
      intervals: [
        { interval_datetime: '2025-01-15T10:30:00+10:00', marginalvalue: 50, rhs: 100, violationdegree: 0 },
      ],
    },
  ],
};

const emptyInterconnectors = { run_datetime: null, data: {} };
const emptyConstraints = { run_datetime: null, constraints: [] };

const mockUnits = {
  days: 14,
  units: [
    { duid: 'BAYSW1', quality: 'good', n: 412, observed_corr: 0.95, mae: 5.2, tracking: true },
    { duid: 'LDBESS1', quality: 'good', n: 380, observed_corr: 0.02, mae: 40.1, tracking: false },
  ],
  message: '2 DUIDs with stored inference over 14d',
};

const mockSeries = {
  duid: 'BAYSW1',
  days: 14,
  data: [
    { interval_datetime: '2025-01-15T10:30:00+10:00', mw_inferred: 500, mw_realised: 510 },
    { interval_datetime: '2025-01-15T11:00:00+10:00', mw_inferred: 520, mw_realised: 515 },
  ],
  stats: { n: 2, corr: 0.95, mae: 5.2, quality: 'good', median_n_equations: 6 },
  message: '2 paired intervals for BAYSW1 over 14d',
};

const emptyUnits = { days: 14, units: [] };

const setupMocks = ({ interconnectors, constraints, units, series }) => {
  axios.get.mockImplementation((url) => {
    if (url.includes('/api/network/interconnectors')) {
      return Promise.resolve({ data: interconnectors });
    }
    if (url.includes('/api/network/constraints')) {
      return Promise.resolve({ data: constraints });
    }
    if (url.includes('/api/network/unit-inference/units')) {
      return Promise.resolve({ data: units || emptyUnits });
    }
    if (url.includes('/api/network/unit-inference/series')) {
      return Promise.resolve({ data: series || mockSeries });
    }
    return Promise.resolve({ data: {} });
  });
};

describe('NetworkPage', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('shows loading state initially', () => {
    axios.get.mockImplementation(() => new Promise(() => {}));
    render(<NetworkPage darkMode={false} />);
    expect(screen.getAllByText(/Loading/i).length).toBeGreaterThan(0);
  });

  test('renders interconnector ribbons and constraint heatmap when populated', async () => {
    setupMocks({ interconnectors: mockInterconnectors, constraints: mockConstraints });
    render(<NetworkPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getAllByTestId('plotly-chart').length).toBe(2);
    });

    expect(screen.getByText(/Interconnector flows vs limits/i)).toBeInTheDocument();
    expect(screen.getByText(/Binding constraints/i)).toBeInTheDocument();
  });

  test('shows empty state when no interconnector or constraint data is available', async () => {
    setupMocks({ interconnectors: emptyInterconnectors, constraints: emptyConstraints });
    render(<NetworkPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByText(/No interconnector data available yet/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/No binding constraints in the latest pre-dispatch run/i)).toBeInTheDocument();
  });

  test('shows an error message when the interconnector fetch fails', async () => {
    axios.get.mockImplementation((url) => {
      if (url.includes('/api/network/interconnectors')) {
        return Promise.reject({ response: { data: { detail: 'boom' } } });
      }
      if (url.includes('/api/network/unit-inference')) {
        return Promise.resolve({ data: emptyUnits });
      }
      return Promise.resolve({ data: emptyConstraints });
    });
    render(<NetworkPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByText('boom')).toBeInTheDocument();
    });
  });

  test('clicking a category toggle refetches constraints with the new category', async () => {
    setupMocks({ interconnectors: emptyInterconnectors, constraints: mockConstraints });
    render(<NetworkPage darkMode={false} />);

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledWith(
        '/api/network/constraints',
        expect.objectContaining({ params: expect.objectContaining({ category: 'all' }) })
      );
    });

    fireEvent.click(screen.getByText('Network'));

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledWith(
        '/api/network/constraints',
        expect.objectContaining({ params: expect.objectContaining({ category: 'network' }) })
      );
    });
  });

  test('applies dark mode class', async () => {
    setupMocks({ interconnectors: emptyInterconnectors, constraints: emptyConstraints });
    render(<NetworkPage darkMode={true} />);

    await waitFor(() => {
      expect(document.querySelector('.network-container')).toHaveClass('dark');
    });
  });

  test('shows empty state when no unit-inference rows are stored', async () => {
    setupMocks({ interconnectors: emptyInterconnectors, constraints: emptyConstraints, units: emptyUnits });
    render(<NetworkPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByText(/No stored unit-inference rows yet/i)).toBeInTheDocument();
    });
  });

  test('lists DUIDs labelled with corr and n, selects the top one, and renders its paired series', async () => {
    setupMocks({
      interconnectors: emptyInterconnectors, constraints: emptyConstraints, units: mockUnits, series: mockSeries,
    });
    render(<NetworkPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByText(/BAYSW1 · corr 0\.95 · n=412/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/LDBESS1 · corr 0\.02 · n=380/i)).toBeInTheDocument();

    await waitFor(() => {
      expect(screen.getByTestId('plotly-chart')).toBeInTheDocument();
    });
    expect(document.querySelector('.unit-stats-chip').textContent).toMatch(/corr 0\.95/i);
    expect(screen.getByText(/MAE 5\.2 MW/i)).toBeInTheDocument();
    expect(screen.getByText(/Median equations per solve: 6/i)).toBeInTheDocument();
    // Provenance lines from both endpoints' message fields.
    expect(screen.getByText('2 DUIDs with stored inference over 14d')).toBeInTheDocument();
    expect(screen.getByText('2 paired intervals for BAYSW1 over 14d')).toBeInTheDocument();
  });

  test('clicking a non-tracking DUID fetches its series', async () => {
    setupMocks({
      interconnectors: emptyInterconnectors, constraints: emptyConstraints, units: mockUnits, series: mockSeries,
    });
    render(<NetworkPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByText(/LDBESS1 · corr 0\.02 · n=380/i)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/LDBESS1 · corr 0\.02 · n=380/i));

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledWith(
        '/api/network/unit-inference/series',
        expect.objectContaining({ params: expect.objectContaining({ duid: 'LDBESS1' }) })
      );
    });
  });
});
