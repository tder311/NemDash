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

const setupMocks = ({ interconnectors, constraints }) => {
  axios.get.mockImplementation((url) => {
    if (url.includes('/api/network/interconnectors')) {
      return Promise.resolve({ data: interconnectors });
    }
    if (url.includes('/api/network/constraints')) {
      return Promise.resolve({ data: constraints });
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
});
