import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import App from '../App';
import axios from 'axios';
import { setupAxiosMocks, resetAxiosMocks } from '../mocks/handlers';

// Setup axios mocks
beforeEach(() => {
  setupAxiosMocks(axios);
});

afterEach(() => {
  resetAxiosMocks(axios);
});

describe('App', () => {
  test('renders header with title', async () => {
    render(<App />);

    await waitFor(() => {
      expect(screen.getByText(/NEM Market Dashboard/i)).toBeInTheDocument();
    });
  });

  test('renders dark mode toggle', async () => {
    render(<App />);

    await waitFor(() => {
      expect(screen.getByText('🌙')).toBeInTheDocument();
    });
  });

  test('toggles dark mode when clicked', async () => {
    render(<App />);

    await waitFor(() => {
      const appElement = document.querySelector('.app');
      expect(appElement).toHaveClass('light');
    });

    const toggle = document.querySelector('.toggle-switch');
    fireEvent.click(toggle);

    await waitFor(() => {
      const appElement = document.querySelector('.app');
      expect(appElement).toHaveClass('dark');
    });
  });

  test('renders Live Prices tab by default', async () => {
    render(<App />);

    await waitFor(() => {
      const liveTab = screen.getByText(/Live Prices & Flows/i);
      expect(liveTab.closest('.tab')).toHaveClass('active');
    });
  });

  test('switches to Price History tab when clicked', async () => {
    render(<App />);

    // Wait for initial render
    await waitFor(() => {
      expect(screen.getByText(/Price History/i)).toBeInTheDocument();
    });

    const historyTab = screen.getByText(/Price History/i);
    fireEvent.click(historyTab);

    await waitFor(() => {
      expect(historyTab.closest('.tab')).toHaveClass('active');
    });
  });

  test('applies dark class to body when dark mode enabled', async () => {
    render(<App />);

    await waitFor(() => {
      expect(document.body).toHaveClass('light');
    });

    const toggle = document.querySelector('.toggle-switch');
    fireEvent.click(toggle);

    await waitFor(() => {
      expect(document.body).toHaveClass('dark');
    });
  });

  test('toggle switch has active class when dark mode is on', async () => {
    render(<App />);

    const toggle = document.querySelector('.toggle-switch');
    expect(toggle).not.toHaveClass('active');

    fireEvent.click(toggle);

    await waitFor(() => {
      expect(toggle).toHaveClass('active');
    });
  });
});

describe('App navigation', () => {
  test('shows LivePricesPage when live tab is active', async () => {
    render(<App />);

    // Wait for loading to complete
    await waitFor(() => {
      expect(screen.queryByText(/Loading market data/i)).not.toBeInTheDocument();
    }, { timeout: 3000 });

    // Should show live prices content
    await waitFor(() => {
      expect(screen.getByText(/Last Updated/i)).toBeInTheDocument();
    });
  });

  test('shows PriceHistoryPage when history tab is clicked', async () => {
    render(<App />);

    // Wait for initial load
    await waitFor(() => {
      expect(screen.getByText(/Price History/i)).toBeInTheDocument();
    });

    const historyTab = screen.getByText(/Price History/i);
    fireEvent.click(historyTab);

    // Price history page content should be visible
    // (It will show loading or chart)
    await waitFor(() => {
      const historyTabElement = historyTab.closest('.tab');
      expect(historyTabElement).toHaveClass('active');
    });
  });
});
