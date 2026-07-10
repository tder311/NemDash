import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import axios from 'axios';
import GenerationForecastPage from '../../components/GenerationForecastPage';

const mockResponse = {
  run_datetime: '2026-07-10T09:00:00+10:00',
  units: [
    {
      duid: 'COAL1', station_name: 'Big Station', fuel_source: 'Coal', technology_type: 'Steam Turbine',
      capacity_mw: 700,
      series: [
        { interval_datetime: '2026-07-10T09:30:00+10:00', mw: 500, quality: 'good' },
        { interval_datetime: '2026-07-10T10:30:00+10:00', mw: 520, quality: 'good' },
      ],
    },
    {
      duid: 'BESS1', station_name: 'Battery One', fuel_source: 'Battery', technology_type: 'BESS',
      capacity_mw: 100,
      series: [
        { interval_datetime: '2026-07-10T09:30:00+10:00', mw: 80, quality: 'weak' },
      ],
    },
  ],
  fleets: [
    {
      fuel_source: 'Wind', n_units_total: 4, capacity_total: 400,
      series: [
        { interval_datetime: '2026-07-10T09:30:00+10:00', mw_sum: 150, n_units: 3, capacity_inferable: 300 },
        { interval_datetime: '2026-07-10T10:30:00+10:00', mw_sum: 200, n_units: 4, capacity_inferable: 400 },
      ],
    },
    {
      fuel_source: 'Solar', n_units_total: 2, capacity_total: 150,
      series: [
        { interval_datetime: '2026-07-10T09:30:00+10:00', mw_sum: 90, n_units: 2, capacity_inferable: 150 },
      ],
    },
  ],
  message: '2 units, 2 fleets for NSW1 from run 2026-07-10T09:00:00+10:00',
};

const emptyResponse = { run_datetime: null, units: [], fleets: [], message: '' };

const setupMocks = (response) => {
  axios.get.mockImplementation((url) => {
    if (url.includes('/api/network/generation-forecast')) {
      return Promise.resolve({ data: response });
    }
    return Promise.resolve({ data: {} });
  });
};

describe('GenerationForecastPage', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('shows loading state initially', () => {
    axios.get.mockImplementation(() => new Promise(() => {}));
    render(<GenerationForecastPage darkMode={false} />);
    expect(screen.getByText(/Loading generation forecast/i)).toBeInTheDocument();
  });

  test('renders coal, gas, battery heatmaps and the wind+solar fleet chart when populated', async () => {
    setupMocks(mockResponse);
    render(<GenerationForecastPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getAllByTestId('plotly-chart').length).toBe(3);
    });

    expect(screen.getByText('Coal')).toBeInTheDocument();
    expect(screen.getByText('Gas')).toBeInTheDocument();
    expect(screen.getByText('Battery')).toBeInTheDocument();
    expect(screen.getByText('Wind + Solar (fleet)')).toBeInTheDocument();
    // Gas has no units in the mock -- its section should show the empty message.
    expect(screen.getByText(/No gas units inferable/i)).toBeInTheDocument();
  });

  test('shows the battery discharge-only caveat', async () => {
    setupMocks(mockResponse);
    render(<GenerationForecastPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByText(/discharge-only inference/i)).toBeInTheDocument();
    });
  });

  test('shows fleet coverage subtitles for wind and solar', async () => {
    setupMocks(mockResponse);
    render(<GenerationForecastPage darkMode={false} />);

    await waitFor(() => {
      // Wind has 2 series points; the median (upper-middle) index is 1: 4 of 4 units, 100% capacity.
      expect(screen.getByText(/Wind: 4 of 4 units/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/Solar: 2 of 2 units/i)).toBeInTheDocument();
  });

  test('shows empty states when no data is available', async () => {
    setupMocks(emptyResponse);
    render(<GenerationForecastPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByText(/No coal units inferable/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/No gas units inferable/i)).toBeInTheDocument();
    expect(screen.getByText(/No battery units inferable/i)).toBeInTheDocument();
    expect(screen.getByText(/No wind or solar fleet data/i)).toBeInTheDocument();
  });

  test('shows an error message when the fetch fails', async () => {
    axios.get.mockImplementation(() => Promise.reject({ response: { data: { detail: 'boom' } } }));
    render(<GenerationForecastPage darkMode={false} />);

    await waitFor(() => {
      expect(screen.getByText('boom')).toBeInTheDocument();
    });
  });

  test('applies dark mode class', async () => {
    setupMocks(emptyResponse);
    render(<GenerationForecastPage darkMode={true} />);

    await waitFor(() => {
      expect(document.querySelector('.generation-container')).toHaveClass('dark');
    });
  });

  test('changing the region refetches the forecast', async () => {
    setupMocks(mockResponse);
    render(<GenerationForecastPage darkMode={false} />);

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledWith(
        '/api/network/generation-forecast',
        expect.objectContaining({ params: expect.objectContaining({ region: 'NSW1' }) })
      );
    });

    fireEvent.change(screen.getByLabelText('Region'), { target: { value: 'VIC1' } });

    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledWith(
        '/api/network/generation-forecast',
        expect.objectContaining({ params: expect.objectContaining({ region: 'VIC1' }) })
      );
    });
  });
});
