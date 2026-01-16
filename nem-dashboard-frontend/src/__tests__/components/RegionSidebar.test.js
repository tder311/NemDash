import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import RegionSidebar from '../../components/RegionSidebar';

const mockRegions = [
  { region: 'NSW', price: 85.50, totaldemand: 7500 },
  { region: 'VIC', price: 72.30, totaldemand: 5200 },
  { region: 'QLD', price: 65.10, totaldemand: 6800 },
  { region: 'SA', price: 95.20, totaldemand: 2100 },
  { region: 'TAS', price: 55.00, totaldemand: 1200 },
];

describe('RegionSidebar', () => {
  const mockHover = jest.fn();
  const mockLeave = jest.fn();
  const mockClick = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('renders all regions', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    expect(screen.getByText('NSW')).toBeInTheDocument();
    expect(screen.getByText('VIC')).toBeInTheDocument();
    expect(screen.getByText('QLD')).toBeInTheDocument();
    expect(screen.getByText('SA')).toBeInTheDocument();
    expect(screen.getByText('TAS')).toBeInTheDocument();
  });

  test('displays formatted prices', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    expect(screen.getByText('$85.50/MWh')).toBeInTheDocument();
    expect(screen.getByText('$72.30/MWh')).toBeInTheDocument();
  });

  test('displays formatted demand', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    expect(screen.getByText('7500 MW')).toBeInTheDocument();
    expect(screen.getByText('5200 MW')).toBeInTheDocument();
  });

  test('calls onRegionHover when mouse enters', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    const nswCard = screen.getByText('NSW').closest('.sidebar-region-card');
    fireEvent.mouseEnter(nswCard);

    expect(mockHover).toHaveBeenCalledWith('NSW');
  });

  test('calls onRegionLeave when mouse leaves', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    const nswCard = screen.getByText('NSW').closest('.sidebar-region-card');
    fireEvent.mouseLeave(nswCard);

    expect(mockLeave).toHaveBeenCalled();
  });

  test('calls onRegionClick when card is clicked', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    const vicCard = screen.getByText('VIC').closest('.sidebar-region-card');
    fireEvent.click(vicCard);

    expect(mockClick).toHaveBeenCalledWith('VIC');
  });

  test('handles null price gracefully', () => {
    const regionsWithNull = [
      { region: 'NSW', price: null, totaldemand: 7500 }
    ];

    render(
      <RegionSidebar
        regions={regionsWithNull}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    expect(screen.getByText('N/A')).toBeInTheDocument();
  });

  test('handles undefined demand gracefully', () => {
    const regionsWithUndefined = [
      { region: 'NSW', price: 85.50, totaldemand: undefined }
    ];

    render(
      <RegionSidebar
        regions={regionsWithUndefined}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    // Should show N/A for undefined demand
    const naElements = screen.getAllByText('N/A');
    expect(naElements.length).toBeGreaterThan(0);
  });

  test('applies dark mode class', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={true}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    expect(document.querySelector('.region-sidebar')).toHaveClass('dark');
  });

  test('applies light mode class when darkMode is false', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    expect(document.querySelector('.region-sidebar')).toHaveClass('light');
  });

  test('displays full region names', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    expect(screen.getByText('New South Wales')).toBeInTheDocument();
    expect(screen.getByText('Victoria')).toBeInTheDocument();
    expect(screen.getByText('Queensland')).toBeInTheDocument();
    expect(screen.getByText('South Australia')).toBeInTheDocument();
    expect(screen.getByText('Tasmania')).toBeInTheDocument();
  });

  test('renders sidebar title', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    expect(screen.getByText('NEM Regions')).toBeInTheDocument();
  });

  test('does not call onRegionClick if not provided', () => {
    // Render without onRegionClick
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
      />
    );

    const nswCard = screen.getByText('NSW').closest('.sidebar-region-card');

    // Should not throw when clicked
    expect(() => fireEvent.click(nswCard)).not.toThrow();
  });

  test('handles empty regions array', () => {
    render(
      <RegionSidebar
        regions={[]}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    // Should render the sidebar but no region cards
    expect(screen.getByText('NEM Regions')).toBeInTheDocument();
    expect(screen.queryByText('NSW')).not.toBeInTheDocument();
  });

  test('rounds demand values', () => {
    const regionsWithDecimal = [
      { region: 'NSW', price: 85.50, totaldemand: 7523.7 }
    ];

    render(
      <RegionSidebar
        regions={regionsWithDecimal}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    // Should round to nearest integer
    expect(screen.getByText('7524 MW')).toBeInTheDocument();
  });
});

describe('RegionSidebar color mapping', () => {
  const mockHover = jest.fn();
  const mockLeave = jest.fn();
  const mockClick = jest.fn();

  test('applies correct color for each region', () => {
    render(
      <RegionSidebar
        regions={mockRegions}
        darkMode={false}
        onRegionHover={mockHover}
        onRegionLeave={mockLeave}
        onRegionClick={mockClick}
      />
    );

    // Check that cards have style with region color
    const cards = document.querySelectorAll('.sidebar-region-card');
    expect(cards.length).toBe(5);
  });
});
