import React, { useState, useEffect } from 'react';
import './App.css';
import LivePricesPage from './components/LivePricesPage';

function App() {
  const [darkMode, setDarkMode] = useState(false);

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
        <LivePricesPage darkMode={darkMode} />
      </div>
    </div>
  );
}

export default App;