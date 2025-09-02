import React from 'react';
import './RegionCard.css';

function RegionCard({ region, price, demand, color, position, darkMode }) {
  return (
    <div 
      className={`region-card ${darkMode ? 'dark' : 'light'}`}
      style={{
        position: 'absolute',
        ...position,
        '--region-color': color
      }}
    >
      <div className="region-name" style={{ color }}>
        {region}
      </div>
      <div className="region-price">
        ${price?.toFixed(2)}/MWh
      </div>
      <div className="region-demand">
        {demand?.toLocaleString()} MW
      </div>
    </div>
  );
}

export default RegionCard;