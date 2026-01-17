import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import AustraliaMap from '../../components/AustraliaMap';

// Mock fetch for SVG loading
const mockSvgContent = `
<svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
  <path id="state-NSW" class="state-path" d="M10,10 L20,10 L20,20 L10,20 Z" />
  <path id="state-VIC" class="state-path" d="M30,10 L40,10 L40,20 L30,20 Z" />
  <path id="state-QLD" class="state-path" d="M50,10 L60,10 L60,20 L50,20 Z" />
  <path id="state-SA" class="state-path" d="M70,10 L80,10 L80,20 L70,20 Z" />
  <path id="state-TAS" class="state-path" d="M90,10 L100,10 L100,20 L90,20 Z" />
  <path id="state-WA" class="state-path" d="M0,50 L10,50 L10,60 L0,60 Z" />
  <path id="state-NT" class="state-path" d="M20,50 L30,50 L30,60 L20,60 Z" />
</svg>
`;

describe('AustraliaMap', () => {
  const mockOnRegionClick = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    global.fetch = jest.fn(() =>
      Promise.resolve({
        text: () => Promise.resolve(mockSvgContent),
      })
    );
  });

  afterEach(() => {
    // Restore default fetch mock - don't delete global.fetch as it affects other tests
    global.fetch.mockClear();
  });

  test('fetches SVG content on mount', async () => {
    render(<AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />);

    await waitFor(() => {
      expect(global.fetch).toHaveBeenCalledWith('/australia-map.svg');
    });
  });

  test('renders SVG content after loading', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });
  });

  test('handles SVG fetch error gracefully', async () => {
    global.fetch = jest.fn(() => Promise.reject(new Error('Failed to load')));

    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      // Should not throw, just log error
      expect(container.querySelector('svg')).not.toBeInTheDocument();
    });
  });

  test('calls onRegionClick when NEM region is clicked', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('#state-NSW')).toBeInTheDocument();
    });

    const nswPath = container.querySelector('#state-NSW');
    fireEvent.click(nswPath);

    expect(mockOnRegionClick).toHaveBeenCalledWith('NSW');
  });

  test('calls onRegionClick for all NEM regions', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    const nemRegions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS'];

    for (const region of nemRegions) {
      mockOnRegionClick.mockClear();
      const path = container.querySelector(`#state-${region}`);
      fireEvent.click(path);
      expect(mockOnRegionClick).toHaveBeenCalledWith(region);
    }
  });

  test('does not call onRegionClick for non-NEM regions', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    // Click on WA (not in NEM)
    const waPath = container.querySelector('#state-WA');
    fireEvent.click(waPath);

    expect(mockOnRegionClick).not.toHaveBeenCalled();

    // Click on NT (not in NEM)
    const ntPath = container.querySelector('#state-NT');
    fireEvent.click(ntPath);

    expect(mockOnRegionClick).not.toHaveBeenCalled();
  });

  test('does not call onRegionClick if handler not provided', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    const nswPath = container.querySelector('#state-NSW');
    // Should not throw
    expect(() => fireEvent.click(nswPath)).not.toThrow();
  });

  test('adds pointer cursor to NEM region paths', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('#state-NSW')).toBeInTheDocument();
    });

    const nemRegions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS'];

    for (const region of nemRegions) {
      const path = container.querySelector(`#state-${region}`);
      expect(path.style.cursor).toBe('pointer');
    }
  });

  test('does not add pointer cursor to non-NEM regions', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    const waPath = container.querySelector('#state-WA');
    expect(waPath.style.cursor).not.toBe('pointer');
  });

  test('highlights region when hoveredRegion prop is set', async () => {
    const { container, rerender } = render(
      <AustraliaMap darkMode={false} hoveredRegion={null} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    // Initially no highlighting
    expect(container.querySelector('#state-NSW').classList.contains('highlighted')).toBe(false);

    // Rerender with hoveredRegion
    rerender(
      <AustraliaMap darkMode={false} hoveredRegion="NSW" onRegionClick={mockOnRegionClick} />
    );

    expect(container.querySelector('#state-NSW').classList.contains('highlighted')).toBe(true);
  });

  test('removes highlighting when hoveredRegion is cleared', async () => {
    const { container, rerender } = render(
      <AustraliaMap darkMode={false} hoveredRegion="NSW" onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    expect(container.querySelector('#state-NSW').classList.contains('highlighted')).toBe(true);

    // Clear hoveredRegion
    rerender(
      <AustraliaMap darkMode={false} hoveredRegion={null} onRegionClick={mockOnRegionClick} />
    );

    expect(container.querySelector('#state-NSW').classList.contains('highlighted')).toBe(false);
  });

  test('only highlights one region at a time', async () => {
    const { container, rerender } = render(
      <AustraliaMap darkMode={false} hoveredRegion="NSW" onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    expect(container.querySelector('#state-NSW').classList.contains('highlighted')).toBe(true);
    expect(container.querySelector('#state-VIC').classList.contains('highlighted')).toBe(false);

    // Change to VIC
    rerender(
      <AustraliaMap darkMode={false} hoveredRegion="VIC" onRegionClick={mockOnRegionClick} />
    );

    expect(container.querySelector('#state-NSW').classList.contains('highlighted')).toBe(false);
    expect(container.querySelector('#state-VIC').classList.contains('highlighted')).toBe(true);
  });

  test('applies dark mode filter', async () => {
    const { container } = render(
      <AustraliaMap darkMode={true} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    const containerDiv = container.firstChild;
    expect(containerDiv.style.filter).toBe('invert(1) hue-rotate(180deg)');
  });

  test('does not apply filter in light mode', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    const containerDiv = container.firstChild;
    expect(containerDiv.style.filter).toBe('none');
  });

  test('applies correct positioning styles', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    const containerDiv = container.firstChild;
    expect(containerDiv.style.position).toBe('absolute');
    expect(containerDiv.style.top).toBe('50%');
    expect(containerDiv.style.left).toBe('50%');
    expect(containerDiv.style.transform).toBe('translate(-50%, -50%)');
  });

  test('cleans up event listener on unmount', async () => {
    const { container, unmount } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    // Store reference to container element before unmount
    const containerDiv = container.firstChild;
    const removeEventListenerSpy = jest.spyOn(containerDiv, 'removeEventListener');

    unmount();

    expect(removeEventListenerSpy).toHaveBeenCalledWith('click', expect.any(Function));
  });

  test('handles click on container (not on state path)', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={mockOnRegionClick} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    // Click on the container itself, not a state path
    fireEvent.click(container.firstChild);

    expect(mockOnRegionClick).not.toHaveBeenCalled();
  });
});

describe('AustraliaMap hover events', () => {
  const mockOnRegionClick = jest.fn();
  const mockOnRegionHover = jest.fn();
  const mockOnRegionLeave = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    global.fetch = jest.fn(() =>
      Promise.resolve({
        text: () => Promise.resolve(mockSvgContent),
      })
    );
  });

  test('calls onRegionHover when mouse enters NEM region', async () => {
    const { container } = render(
      <AustraliaMap
        darkMode={false}
        onRegionClick={mockOnRegionClick}
        onRegionHover={mockOnRegionHover}
        onRegionLeave={mockOnRegionLeave}
      />
    );

    await waitFor(() => {
      expect(container.querySelector('#state-NSW')).toBeInTheDocument();
    });

    const nswPath = container.querySelector('#state-NSW');
    fireEvent.mouseOver(nswPath);

    expect(mockOnRegionHover).toHaveBeenCalledWith('NSW');
  });

  test('calls onRegionLeave when mouse leaves NEM region', async () => {
    const { container } = render(
      <AustraliaMap
        darkMode={false}
        onRegionClick={mockOnRegionClick}
        onRegionHover={mockOnRegionHover}
        onRegionLeave={mockOnRegionLeave}
      />
    );

    await waitFor(() => {
      expect(container.querySelector('#state-NSW')).toBeInTheDocument();
    });

    const nswPath = container.querySelector('#state-NSW');
    fireEvent.mouseOut(nswPath);

    expect(mockOnRegionLeave).toHaveBeenCalled();
  });

  test('does not call onRegionHover for non-NEM regions', async () => {
    const { container } = render(
      <AustraliaMap
        darkMode={false}
        onRegionClick={mockOnRegionClick}
        onRegionHover={mockOnRegionHover}
        onRegionLeave={mockOnRegionLeave}
      />
    );

    await waitFor(() => {
      expect(container.querySelector('#state-WA')).toBeInTheDocument();
    });

    const waPath = container.querySelector('#state-WA');
    fireEvent.mouseOver(waPath);

    expect(mockOnRegionHover).not.toHaveBeenCalled();
  });

  test('applies region-colored stroke when highlighting', async () => {
    const { container, rerender } = render(
      <AustraliaMap
        darkMode={false}
        hoveredRegion={null}
        onRegionClick={mockOnRegionClick}
      />
    );

    await waitFor(() => {
      expect(container.querySelector('#state-NSW')).toBeInTheDocument();
    });

    // Rerender with NSW hovered
    rerender(
      <AustraliaMap
        darkMode={false}
        hoveredRegion="NSW"
        onRegionClick={mockOnRegionClick}
      />
    );

    const nswPath = container.querySelector('#state-NSW');
    expect(nswPath.style.stroke).toBe('#1f77b4');
    expect(nswPath.style.strokeWidth).toBe('3px');
  });

  test('clears stroke when highlighting is removed', async () => {
    const { container, rerender } = render(
      <AustraliaMap
        darkMode={false}
        hoveredRegion="NSW"
        onRegionClick={mockOnRegionClick}
      />
    );

    await waitFor(() => {
      expect(container.querySelector('#state-NSW')).toBeInTheDocument();
    });

    // Clear hoveredRegion
    rerender(
      <AustraliaMap
        darkMode={false}
        hoveredRegion={null}
        onRegionClick={mockOnRegionClick}
      />
    );

    const nswPath = container.querySelector('#state-NSW');
    expect(nswPath.style.stroke).toBe('');
    expect(nswPath.style.strokeWidth).toBe('');
  });

  test('each region gets its correct color on hover', async () => {
    const regionColors = {
      'NSW': '#1f77b4',
      'VIC': '#ff7f0e',
      'QLD': '#2ca02c',
      'SA': '#d62728',
      'TAS': '#9467bd',
    };

    const { container, rerender } = render(
      <AustraliaMap
        darkMode={false}
        hoveredRegion={null}
        onRegionClick={mockOnRegionClick}
      />
    );

    await waitFor(() => {
      expect(container.querySelector('#state-NSW')).toBeInTheDocument();
    });

    for (const [region, expectedColor] of Object.entries(regionColors)) {
      rerender(
        <AustraliaMap
          darkMode={false}
          hoveredRegion={region}
          onRegionClick={mockOnRegionClick}
        />
      );

      const path = container.querySelector(`#state-${region}`);
      expect(path.style.stroke).toBe(expectedColor);
    }
  });
});

describe('AustraliaMap edge cases', () => {
  beforeEach(() => {
    global.fetch = jest.fn(() =>
      Promise.resolve({
        text: () => Promise.resolve(mockSvgContent),
      })
    );
  });

  afterEach(() => {
    // Restore default fetch mock - don't delete global.fetch as it affects other tests
    global.fetch.mockClear();
  });

  test('handles invalid hoveredRegion gracefully', async () => {
    const { container } = render(
      <AustraliaMap darkMode={false} hoveredRegion="INVALID" onRegionClick={jest.fn()} />
    );

    await waitFor(() => {
      expect(container.querySelector('svg')).toBeInTheDocument();
    });

    // Should not throw, no elements should be highlighted
    const highlighted = container.querySelectorAll('.highlighted');
    expect(highlighted.length).toBe(0);
  });

  test('handles empty SVG response', async () => {
    global.fetch = jest.fn(() =>
      Promise.resolve({
        text: () => Promise.resolve(''),
      })
    );

    const { container } = render(
      <AustraliaMap darkMode={false} onRegionClick={jest.fn()} />
    );

    await waitFor(() => {
      // Should render empty container without error
      expect(container.firstChild).toBeInTheDocument();
    });
  });
});
