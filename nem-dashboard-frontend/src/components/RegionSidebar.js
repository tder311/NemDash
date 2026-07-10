import React from 'react';
import { REGION_COLORS } from '../theme';
import './RegionSidebar.css';

const RegionSidebar = ({ regions, darkMode, hoveredRegion, onRegionHover, onRegionLeave, onRegionClick }) => {
  const handleMouseEnter = (regionCode) => {
    onRegionHover(regionCode);
  };

  const handleMouseLeave = () => {
    onRegionLeave();
  };

  const handleClick = (regionCode) => {
    if (onRegionClick) {
      onRegionClick(regionCode);
    }
  };

  const formatPrice = (price) => {
    if (price === null || price === undefined) return 'N/A';
    return `$${price.toFixed(2)}/MWh`;
  };

  const formatDemand = (demand) => {
    if (demand === null || demand === undefined) return 'N/A';
    return `${Math.round(demand)} MW`;
  };

  return (
    <div className={`region-sidebar ${darkMode ? 'dark' : 'light'}`}>
      <h3 className="sidebar-title">NEM Regions</h3>
      <div className="region-list">
        {regions.map((region) => (
          <div
            key={region.region}
            className={`sidebar-region-card ${hoveredRegion === region.region ? 'highlighted' : ''}`}
            style={{ '--region-color': getRegionColor(region.region) }}
            onMouseEnter={() => handleMouseEnter(region.region)}
            onMouseLeave={handleMouseLeave}
            onClick={() => handleClick(region.region)}
          >
            <div className="region-header">
              <span className="region-code">{region.region}</span>
              <span className="region-name">{getRegionFullName(region.region)}</span>
            </div>
            <div className="region-data">
              <div className="data-item">
                <span className="data-label">Price</span>
                <span className="data-value price">{formatPrice(region.price)}</span>
              </div>
              <div className="data-item">
                <span className="data-label">Demand</span>
                <span className="data-value demand">{formatDemand(region.totaldemand)}</span>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

const getRegionColor = (regionCode) => REGION_COLORS[regionCode] || '#6e7a90';

const getRegionFullName = (regionCode) => {
  const names = {
    'NSW': 'New South Wales',
    'VIC': 'Victoria',
    'QLD': 'Queensland',
    'SA': 'South Australia', 
    'TAS': 'Tasmania',
    'WA': 'Western Australia',
    'NT': 'Northern Territory'
  };
  return names[regionCode] || regionCode;
};

export default RegionSidebar;