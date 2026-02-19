import React, { useState, useEffect } from 'react';
import api from '../api';
import './DownloadsPage.css';

const DATA_TYPES = [
  { id: 'prices', label: 'Price Data', description: 'Regional electricity prices ($/MWh) and demand (MW)' },
  { id: 'generation', label: 'Generation Data', description: 'SCADA values by generator and fuel source' },
  { id: 'metrics', label: 'Daily Metrics', description: 'Capture rates, capture prices, and TB spreads by region' },
  { id: 'pasa', label: 'PASA Forecasts', description: 'Latest demand forecasts, reserves, and LOR conditions' }
];

const REGIONS = ['NSW', 'VIC', 'QLD', 'SA', 'TAS'];

const MONTHS = [
  { value: 1, label: 'Jan' }, { value: 2, label: 'Feb' }, { value: 3, label: 'Mar' },
  { value: 4, label: 'Apr' }, { value: 5, label: 'May' }, { value: 6, label: 'Jun' },
  { value: 7, label: 'Jul' }, { value: 8, label: 'Aug' }, { value: 9, label: 'Sep' },
  { value: 10, label: 'Oct' }, { value: 11, label: 'Nov' }, { value: 12, label: 'Dec' }
];

function DownloadsPage({ darkMode }) {
  // Data type selection
  const [selectedDataType, setSelectedDataType] = useState('prices');

  // Date range - default to last 7 days
  const now = new Date();
  const weekAgo = new Date(now);
  weekAgo.setDate(weekAgo.getDate() - 7);

  const [startDay, setStartDay] = useState(weekAgo.getDate());
  const [startMonth, setStartMonth] = useState(weekAgo.getMonth() + 1);
  const [startYear, setStartYear] = useState(weekAgo.getFullYear());
  const [endDay, setEndDay] = useState(now.getDate());
  const [endMonth, setEndMonth] = useState(now.getMonth() + 1);
  const [endYear, setEndYear] = useState(now.getFullYear());

  // Filters
  const [selectedRegions, setSelectedRegions] = useState([...REGIONS]);
  const [selectedFuelSources, setSelectedFuelSources] = useState([]);
  const [availableFuelSources, setAvailableFuelSources] = useState([]);
  const [pasaType, setPasaType] = useState('pdpasa');

  // UI state
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState(null);

  // Fetch available options on mount
  useEffect(() => {
    const fetchOptions = async () => {
      try {
        const response = await api.get('/api/export/available-options');
        setAvailableFuelSources(response.data.fuel_sources || []);
      } catch (err) {
        console.error('Error fetching export options:', err);
      }
    };
    fetchOptions();
  }, []);

  // Toggle region selection
  const toggleRegion = (region) => {
    setSelectedRegions(prev =>
      prev.includes(region)
        ? prev.filter(r => r !== region)
        : [...prev, region]
    );
  };

  // Toggle fuel source selection
  const toggleFuelSource = (fuel) => {
    setSelectedFuelSources(prev =>
      prev.includes(fuel)
        ? prev.filter(f => f !== fuel)
        : [...prev, fuel]
    );
  };

  // Format date for API
  const formatDateForApi = (day, month, year, isEnd = false) => {
    const date = new Date(year, month - 1, day, isEnd ? 23 : 0, isEnd ? 59 : 0, isEnd ? 59 : 0);
    return date.toISOString().slice(0, 19);
  };

  // Get days in month
  const getDaysInMonth = (month, year) => {
    return new Date(year, month, 0).getDate();
  };

  // Generate year options (last 5 years)
  const yearOptions = [];
  for (let y = now.getFullYear(); y >= now.getFullYear() - 4; y--) {
    yearOptions.push(y);
  }

  // Handle download
  const handleDownload = async () => {
    setDownloading(true);
    setError(null);

    try {
      const startDate = formatDateForApi(startDay, startMonth, startYear);
      const endDate = formatDateForApi(endDay, endMonth, endYear, true);

      let url;
      const params = new URLSearchParams({
        start_date: startDate,
        end_date: endDate
      });

      if (selectedDataType === 'prices') {
        url = '/api/export/prices';
        if (selectedRegions.length < REGIONS.length && selectedRegions.length > 0) {
          params.append('regions', selectedRegions.join(','));
        }
      } else if (selectedDataType === 'generation') {
        url = '/api/export/generation';
        if (selectedRegions.length < REGIONS.length && selectedRegions.length > 0) {
          params.append('regions', selectedRegions.join(','));
        }
        if (selectedFuelSources.length > 0) {
          params.append('fuel_sources', selectedFuelSources.join(','));
        }
      } else if (selectedDataType === 'metrics') {
        url = '/api/export/metrics';
        if (selectedRegions.length < REGIONS.length && selectedRegions.length > 0) {
          params.append('regions', selectedRegions.join(','));
        }
      } else if (selectedDataType === 'pasa') {
        // PASA doesn't use date range - it's always the latest forecast
        url = '/api/export/pasa';
        params.delete('start_date');
        params.delete('end_date');
        params.append('pasa_type', pasaType);
        if (selectedRegions.length < REGIONS.length && selectedRegions.length > 0) {
          params.append('regions', selectedRegions.join(','));
        }
      }

      // Trigger download
      const response = await api.get(`${url}?${params.toString()}`, {
        responseType: 'blob'
      });

      // Create download link
      const blob = new Blob([response.data], { type: 'text/csv' });
      const downloadUrl = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = downloadUrl;

      // Extract filename from Content-Disposition header or generate one
      const contentDisposition = response.headers['content-disposition'];
      let filename = `nem_${selectedDataType}_export.csv`;
      if (contentDisposition) {
        const match = contentDisposition.match(/filename=(.+)/);
        if (match) filename = match[1];
      }

      link.setAttribute('download', filename);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(downloadUrl);

    } catch (err) {
      console.error('Download error:', err);
      setError('Failed to download data. Please try again.');
    } finally {
      setDownloading(false);
    }
  };

  return (
    <div className={`downloads-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="downloads-header">
        <h1 className="downloads-title">Data Downloads</h1>
        <p className="downloads-subtitle">Export NEM market data as CSV files</p>
      </div>

      {/* Data Type Selection */}
      <div className="section">
        <h2 className="section-title">1. Select Data Type</h2>
        <div className="data-type-grid">
          {DATA_TYPES.map(type => (
            <div
              key={type.id}
              className={`data-type-card ${selectedDataType === type.id ? 'selected' : ''}`}
              onClick={() => setSelectedDataType(type.id)}
            >
              <div className="data-type-label">{type.label}</div>
              <div className="data-type-description">{type.description}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Date Range Selection - not shown for PASA (which is always latest forecast) */}
      {selectedDataType !== 'pasa' && (
        <div className="section">
          <h2 className="section-title">2. Select Date Range</h2>
          <div className="date-range-selector">
            {/* Start Date */}
            <div className="date-group">
              <label>From:</label>
              <div className="date-dropdowns">
                <select value={startDay} onChange={(e) => setStartDay(Number(e.target.value))}>
                  {Array.from({ length: getDaysInMonth(startMonth, startYear) }, (_, i) => i + 1).map(d => (
                    <option key={d} value={d}>{d}</option>
                  ))}
                </select>
                <select value={startMonth} onChange={(e) => setStartMonth(Number(e.target.value))}>
                  {MONTHS.map(m => (
                    <option key={m.value} value={m.value}>{m.label}</option>
                  ))}
                </select>
                <select value={startYear} onChange={(e) => setStartYear(Number(e.target.value))}>
                  {yearOptions.map(y => (
                    <option key={y} value={y}>{y}</option>
                  ))}
                </select>
              </div>
            </div>

            <span className="date-separator">to</span>

            {/* End Date */}
            <div className="date-group">
              <label>To:</label>
              <div className="date-dropdowns">
                <select value={endDay} onChange={(e) => setEndDay(Number(e.target.value))}>
                  {Array.from({ length: getDaysInMonth(endMonth, endYear) }, (_, i) => i + 1).map(d => (
                    <option key={d} value={d}>{d}</option>
                  ))}
                </select>
                <select value={endMonth} onChange={(e) => setEndMonth(Number(e.target.value))}>
                  {MONTHS.map(m => (
                    <option key={m.value} value={m.value}>{m.label}</option>
                  ))}
                </select>
                <select value={endYear} onChange={(e) => setEndYear(Number(e.target.value))}>
                  {yearOptions.map(y => (
                    <option key={y} value={y}>{y}</option>
                  ))}
                </select>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="section">
        <h2 className="section-title">{selectedDataType === 'pasa' ? '2' : '3'}. Apply Filters</h2>

        {/* Region Filter */}
        <div className="filter-group">
          <label>Regions:</label>
          <div className="checkbox-group">
            {REGIONS.map(region => (
              <label key={region} className="checkbox-label">
                <input
                  type="checkbox"
                  checked={selectedRegions.includes(region)}
                  onChange={() => toggleRegion(region)}
                />
                {region}
              </label>
            ))}
            <button
              className="select-all-btn"
              onClick={() => setSelectedRegions([...REGIONS])}
            >
              Select All
            </button>
            <button
              className="clear-all-btn"
              onClick={() => setSelectedRegions([])}
            >
              Clear
            </button>
          </div>
        </div>

        {/* Fuel Source Filter (generation only) */}
        {selectedDataType === 'generation' && availableFuelSources.length > 0 && (
          <div className="filter-group">
            <label>Fuel Sources (optional):</label>
            <div className="checkbox-group fuel-sources">
              {availableFuelSources.map(fuel => (
                <label key={fuel} className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={selectedFuelSources.includes(fuel)}
                    onChange={() => toggleFuelSource(fuel)}
                  />
                  {fuel}
                </label>
              ))}
            </div>
            <p className="filter-hint">Leave empty to include all fuel sources</p>
          </div>
        )}

        {/* PASA Type Filter */}
        {selectedDataType === 'pasa' && (
          <div className="filter-group">
            <label>Forecast Type:</label>
            <div className="radio-group">
              <label className="radio-label">
                <input
                  type="radio"
                  name="pasaType"
                  value="pdpasa"
                  checked={pasaType === 'pdpasa'}
                  onChange={() => setPasaType('pdpasa')}
                />
                PD-PASA (Pre-Dispatch, ~6 hours ahead)
              </label>
              <label className="radio-label">
                <input
                  type="radio"
                  name="pasaType"
                  value="stpasa"
                  checked={pasaType === 'stpasa'}
                  onChange={() => setPasaType('stpasa')}
                />
                ST-PASA (Short Term, ~6 days ahead)
              </label>
            </div>
          </div>
        )}
      </div>

      {/* Download Button */}
      <div className="download-section">
        {error && <div className="error-message">{error}</div>}
        <button
          className="download-button"
          onClick={handleDownload}
          disabled={downloading || selectedRegions.length === 0}
        >
          {downloading ? (
            <>
              <span className="spinner-small"></span>
              Preparing Download...
            </>
          ) : (
            'Download CSV'
          )}
        </button>
        {selectedRegions.length === 0 && (
          <p className="download-hint">Select at least one region to download</p>
        )}
      </div>
    </div>
  );
}

export default DownloadsPage;
