import React, { useState, useEffect } from 'react';
import './App.css';
import LivePricesPage from './components/LivePricesPage';
import DownloadsPage from './components/DownloadsPage';
import MarketMetricsPage from './components/MarketMetricsPage';
import BidBandPage from './components/BidBandPage';
import ForecastPage from './components/ForecastPage';
import DispatchPage from './components/DispatchPage';
import BidBandsPage from './components/BidBandsPage';
import ChatPage from './components/ChatPage';
import NetworkPage from './components/NetworkPage';
import GenerationForecastPage from './components/GenerationForecastPage';

const TABS = [
  { id: 'dashboard', label: 'Dashboard', Page: LivePricesPage },
  { id: 'metrics', label: 'Market Metrics', Page: MarketMetricsPage },
  { id: 'forecast', label: 'Price Forecast', Page: ForecastPage },
  { id: 'dispatch', label: 'Dispatch', Page: DispatchPage },
  { id: 'bidbands', label: 'Bid Bands', Page: BidBandsPage },
  { id: 'chat', label: 'Ask NemDash', Page: ChatPage },
  { id: 'network', label: 'Network', Page: NetworkPage },
  { id: 'generation', label: 'Generation', Page: GenerationForecastPage },
  { id: 'bids', label: 'Bid Analysis', Page: BidBandPage },
  { id: 'downloads', label: 'Downloads', Page: DownloadsPage },
];

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
        <div className="brand">
          <span className="pulse-dot" aria-hidden="true"></span>
          <h1 className="title">NEM Market Dashboard</h1>
        </div>

        <nav className="main-tabs">
          {TABS.map(({ id, label }) => (
            <button
              key={id}
              className={`tab-button ${activeTab === id ? 'active' : ''}`}
              onClick={() => setActiveTab(id)}
            >
              {label}
            </button>
          ))}
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
        {TABS.map(({ id, Page }) =>
          activeTab === id ? <Page key={id} darkMode={darkMode} /> : null
        )}
      </div>
    </div>
  );
}

export default App;
