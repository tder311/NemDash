import React, { useState, useEffect } from 'react';
import './App.css';
import LivePricesPage from './components/LivePricesPage';
import DownloadsPage from './components/DownloadsPage';
import MarketMetricsPage from './components/MarketMetricsPage';
import BidBandPage from './components/BidBandPage';

function App() {
  const [darkMode, setDarkMode] = useState(true);
  const [activeTab, setActiveTab] = useState('dashboard');

  useEffect(() => {
    document.body.className = darkMode ? 'dark' : 'light';
  }, [darkMode]);

  const toggleDarkMode = () => {
    setDarkMode(!darkMode);
  };

  return (
    <div className={`app ${darkMode ? 'dark' : 'light'}`}>
      <header className="header">
        <h1 className="title">NEM Market Dashboard</h1>

        <nav className="main-tabs">
          <button
            className={`tab-button ${activeTab === 'dashboard' ? 'active' : ''}`}
            onClick={() => setActiveTab('dashboard')}
          >
            Dashboard
          </button>
          <button
            className={`tab-button ${activeTab === 'metrics' ? 'active' : ''}`}
            onClick={() => setActiveTab('metrics')}
          >
            Market Metrics
          </button>
          <button
            className={`tab-button ${activeTab === 'bids' ? 'active' : ''}`}
            onClick={() => setActiveTab('bids')}
          >
            Bid Analysis
          </button>
          <button
            className={`tab-button ${activeTab === 'downloads' ? 'active' : ''}`}
            onClick={() => setActiveTab('downloads')}
          >
            Downloads
          </button>
        </nav>

        <div className="dark-mode-toggle">
          <span className="toggle-label">Dark</span>
          <div
            className={`toggle-switch ${darkMode ? 'active' : ''}`}
            onClick={toggleDarkMode}
          >
            <div className="toggle-slider"></div>
          </div>
        </div>
      </header>

      <div className="content">
        {activeTab === 'dashboard' && <LivePricesPage darkMode={darkMode} />}
        {activeTab === 'metrics' && <MarketMetricsPage darkMode={darkMode} />}
        {activeTab === 'bids' && <BidBandPage darkMode={darkMode} />}
        {activeTab === 'downloads' && <DownloadsPage darkMode={darkMode} />}
      </div>
    </div>
  );
}

export default App;