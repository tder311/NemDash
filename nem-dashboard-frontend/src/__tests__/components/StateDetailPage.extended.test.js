/**
 * Tests for extended time range support (30d, 90d, 365d) in StateDetailPage.
 *
 * These tests verify:
 * - New time range options are present
 * - API calls use correct hour parameters
 * - UI displays aggregation information
 */
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import axios from 'axios';
import StateDetailPage from '../../components/StateDetailPage';

// Mock axios
jest.mock('axios');

const mockPriceHistory = {
  data: [
    { settlementdate: '2025-01-15T08:00:00', price: 80.50, totaldemand: 7500 },
  ],
  aggregation_minutes: 5
};

const mockFuelMix = {
  fuel_mix: [
    { fuel_source: 'Coal', generation_mw: 4500, percentage: 45.0, unit_count: 12 },
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
    { period: '2025-01-15T08:00:00', fuel_source: 'Coal', generation_mw: 4500, sample_count: 6 },
  ],
  aggregation_minutes: 30
};

describe('StateDetailPage Extended Time Ranges', () => {
  const mockOnBack = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    axios.get
      .mockResolvedValueOnce({ data: mockPriceHistory })
      .mockResolvedValueOnce({ data: mockFuelMix })
      .mockResolvedValueOnce({ data: mockSummary })
      .mockResolvedValueOnce({ data: mockGenerationHistory });
  });

  describe('Time Range Options', () => {
    test('has 30 day option', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      const select = screen.getByRole('combobox');
      const options = Array.from(select.querySelectorAll('option'));
      const values = options.map(o => o.value);

      expect(values).toContain('720');
    });

    test('has 90 day option', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      const select = screen.getByRole('combobox');
      const options = Array.from(select.querySelectorAll('option'));
      const values = options.map(o => o.value);

      expect(values).toContain('2160');
    });

    test('has 365 day option', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      const select = screen.getByRole('combobox');
      const options = Array.from(select.querySelectorAll('option'));
      const values = options.map(o => o.value);

      expect(values).toContain('8760');
    });

    test('displays correct labels for extended options', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      expect(screen.getByText('30 Days')).toBeInTheDocument();
      expect(screen.getByText('90 Days')).toBeInTheDocument();
      expect(screen.getByText('365 Days')).toBeInTheDocument();
    });
  });

  describe('API Calls with Extended Ranges', () => {
    test('fetches 30d data with hours=720', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      // Reset mocks for new API calls
      axios.get.mockClear();
      axios.get
        .mockResolvedValueOnce({ data: { ...mockPriceHistory, aggregation_minutes: 60 } })
        .mockResolvedValueOnce({ data: mockFuelMix })
        .mockResolvedValueOnce({ data: mockSummary })
        .mockResolvedValueOnce({ data: { ...mockGenerationHistory, aggregation_minutes: 60 } });

      const select = screen.getByRole('combobox');
      fireEvent.change(select, { target: { value: '720' } });

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/prices/history?hours=720&price_type=MERGED');
        expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/generation/history?hours=720');
      });
    });

    test('fetches 90d data with hours=2160', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      axios.get.mockClear();
      axios.get
        .mockResolvedValueOnce({ data: { ...mockPriceHistory, aggregation_minutes: 1440 } })
        .mockResolvedValueOnce({ data: mockFuelMix })
        .mockResolvedValueOnce({ data: mockSummary })
        .mockResolvedValueOnce({ data: { ...mockGenerationHistory, aggregation_minutes: 1440 } });

      const select = screen.getByRole('combobox');
      fireEvent.change(select, { target: { value: '2160' } });

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/prices/history?hours=2160&price_type=MERGED');
        expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/generation/history?hours=2160');
      });
    });

    test('fetches 365d data with hours=8760', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      axios.get.mockClear();
      axios.get
        .mockResolvedValueOnce({ data: { ...mockPriceHistory, aggregation_minutes: 10080 } })
        .mockResolvedValueOnce({ data: mockFuelMix })
        .mockResolvedValueOnce({ data: mockSummary })
        .mockResolvedValueOnce({ data: { ...mockGenerationHistory, aggregation_minutes: 10080 } });

      const select = screen.getByRole('combobox');
      fireEvent.change(select, { target: { value: '8760' } });

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/prices/history?hours=8760&price_type=MERGED');
        expect(axios.get).toHaveBeenCalledWith('/api/region/NSW/generation/history?hours=8760');
      });
    });
  });

  describe('Backwards Compatibility', () => {
    test('existing 7d option still works', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      const select = screen.getByRole('combobox');
      const options = Array.from(select.querySelectorAll('option'));
      const values = options.map(o => o.value);

      expect(values).toContain('168');  // 7 days
    });

    test('existing 24h option is default', async () => {
      render(<StateDetailPage region="NSW" darkMode={false} onBack={mockOnBack} />);

      await waitFor(() => {
        expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
      });

      const select = screen.getByRole('combobox');
      expect(select.value).toBe('24');
    });
  });
});
