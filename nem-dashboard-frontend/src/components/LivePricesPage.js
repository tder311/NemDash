import React, { useState, useEffect } from 'react';
import axios from 'axios';
import RegionSidebar from './RegionSidebar';
import AustraliaMap from './AustraliaMap';
import './LivePricesPage.css';


function LivePricesPage({ darkMode }) {
  const [prices, setPrices] = useState([]);
  const [lastUpdated, setLastUpdated] = useState('');
  const [loading, setLoading] = useState(true);
  const [hoveredRegion, setHoveredRegion] = useState(null);

  const fetchData = async () => {
    try {
      // Fetch latest trading prices 
      const tradingResponse = await axios.get('/api/prices/latest?price_type=TRADING');
      const tradingData = tradingResponse.data.data || [];
      
      // Fetch dispatch data for demand information
      const dispatchResponse = await axios.get('/api/prices/latest?price_type=DISPATCH');
      const dispatchData = dispatchResponse.data.data || [];
      
      // Combine trading prices with dispatch demand data
      const combinedData = tradingData.map(tradingRow => {
        const dispatchRow = dispatchData.find(d => d.region === tradingRow.region);
        return {
          ...tradingRow,
          totaldemand: dispatchRow ? dispatchRow.totaldemand : 0
        };
      });
      
      setPrices(combinedData);
      
      if (combinedData.length > 0) {
        const latestTime = new Date(combinedData[0].settlementdate);
        setLastUpdated(latestTime.toLocaleTimeString());
      }
      
      setLoading(false);
    } catch (error) {
      console.error('Error fetching data:', error);
      // Set sample data on error
      setPrices([
        { region: 'NSW', price: 65.50, totaldemand: 8500 },
        { region: 'VIC', price: 72.30, totaldemand: 7200 },
        { region: 'QLD', price: 58.90, totaldemand: 6800 },
        { region: 'SA', price: 95.20, totaldemand: 2100 },
        { region: 'TAS', price: 45.70, totaldemand: 1300 }
      ]);
      setLastUpdated(new Date().toLocaleTimeString());
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 30000); // Refresh every 30 seconds
    return () => clearInterval(interval);
  }, []);

  const handleRegionHover = (regionCode) => {
    setHoveredRegion(regionCode);
    // Highlight the corresponding state on the map
    const stateElement = document.querySelector(`#state-${regionCode}`);
    if (stateElement) {
      stateElement.classList.add('highlighted');
    }
  };

  const handleRegionLeave = () => {
    // Remove highlighting from all states
    document.querySelectorAll('.state-path.highlighted').forEach(el => {
      el.classList.remove('highlighted');
    });
    setHoveredRegion(null);
  };

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner"></div>
        <p>Loading market data...</p>
      </div>
    );
  }

  return (
    <div className={`live-prices-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="last-updated">
        Last Updated: {lastUpdated}
      </div>
      
      {/* Left Sidebar with Region List */}
      <RegionSidebar
        regions={prices}
        darkMode={darkMode}
        onRegionHover={handleRegionHover}
        onRegionLeave={handleRegionLeave}
      />
      
      {/* Australian Map */}
      <div className="map-container">
        <AustraliaMap darkMode={darkMode} hoveredRegion={hoveredRegion} />
      </div>
    </div>
  );
}

export default LivePricesPage;