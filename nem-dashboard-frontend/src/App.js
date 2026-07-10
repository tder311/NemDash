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
            className={`tab-button ${activeTab === 'forecast' ? 'active' : ''}`}
            onClick={() => setActiveTab('forecast')}
          >
            Price Forecast
          </button>
          <button
            className={`tab-button ${activeTab === 'dispatch' ? 'active' : ''}`}
            onClick={() => setActiveTab('dispatch')}
          >
            Dispatch
          </button>
          <button
            className={`tab-button ${activeTab === 'bidbands' ? 'active' : ''}`}
            onClick={() => setActiveTab('bidbands')}
          >
            Bid Bands
          </button>
          <button
            className={`tab-button ${activeTab === 'chat' ? 'active' : ''}`}
            onClick={() => setActiveTab('chat')}
          >
            Ask NemDash
          </button>
          <button
            className={`tab-button ${activeTab === 'network' ? 'active' : ''}`}
            onClick={() => setActiveTab('network')}
          >
            Network
          </button>
          <button
            className={`tab-button ${activeTab === 'generation' ? 'active' : ''}`}
            onClick={() => setActiveTab('generation')}
          >
            Generation
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
        {activeTab === 'forecast' && <ForecastPage darkMode={darkMode} />}
        {activeTab === 'dispatch' && <DispatchPage darkMode={darkMode} />}
        {activeTab === 'bidbands' && <BidBandsPage darkMode={darkMode} />}
        {activeTab === 'chat' && <ChatPage darkMode={darkMode} />}
        {activeTab === 'network' && <NetworkPage darkMode={darkMode} />}
        {activeTab === 'generation' && <GenerationForecastPage darkMode={darkMode} />}
        {activeTab === 'bids' && <BidBandPage darkMode={darkMode} />}
        {activeTab === 'downloads' && <DownloadsPage darkMode={darkMode} />}
      </div>
    </div>
  );
}

export default App;