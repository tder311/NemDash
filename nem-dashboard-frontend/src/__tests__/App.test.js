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

  test('renders dark mode toggle with label', async () => {
    render(<App />);

    await waitFor(() => {
      expect(screen.getByText('Dark')).toBeInTheDocument();
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

describe('App layout', () => {
  test('shows LivePricesPage content', async () => {
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

  test('displays NEM Regions sidebar', async () => {
    render(<App />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading market data/i)).not.toBeInTheDocument();
    }, { timeout: 3000 });

    await waitFor(() => {
      expect(screen.getByText(/NEM Regions/i)).toBeInTheDocument();
    });
  });
});
