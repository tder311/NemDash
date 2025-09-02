import React, { useState, useEffect } from 'react';
import './App.css';
import LivePricesPage from './components/LivePricesPage';
import PriceHistoryPage from './components/PriceHistoryPage';

function App() {
  const [darkMode, setDarkMode] = useState(false);
  const [activeTab, setActiveTab] = useState('live');

  useEffect(() => {
    document.body.className = darkMode ? 'dark' : 'light';
  }, [darkMode]);

  const toggleDarkMode = () => {
    setDarkMode(!darkMode);
  };

  return (
    <div className={`app ${darkMode ? 'dark' : 'light'}`}>
      <header className="header">
        <h1 className="title">⚡ NEM Market Dashboard</h1>
        <div className="dark-mode-toggle">
          <span>🌙</span>
          <div 
            className={`toggle-switch ${darkMode ? 'active' : ''}`}
            onClick={toggleDarkMode}
          >
            <div className="toggle-slider"></div>
          </div>
        </div>
      </header>

      <div className="tabs">
        <div 
          className={`tab ${activeTab === 'live' ? 'active' : ''}`}
          onClick={() => setActiveTab('live')}
        >
          🔴 Live Prices & Flows
        </div>
        <div 
          className={`tab ${activeTab === 'history' ? 'active' : ''}`}
          onClick={() => setActiveTab('history')}
        >
          📈 Price History
        </div>
      </div>

      <div className="content">
        {activeTab === 'live' && <LivePricesPage darkMode={darkMode} />}
        {activeTab === 'history' && <PriceHistoryPage darkMode={darkMode} />}
      </div>
    </div>
  );
}

export default App;