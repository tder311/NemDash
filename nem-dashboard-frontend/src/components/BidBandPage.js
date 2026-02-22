import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import Plot from 'react-plotly.js';
import api from '../api';
import './BidBandPage.css';

// Band colors: green (cheap) through yellow/orange to red (expensive)
const BAND_COLORS = [
  '#16a34a', '#22c55e', '#4ade80', '#86efac',
  '#fde047', '#facc15',
  '#fb923c', '#f97316', '#ef4444', '#991b1b'
];

function BidBandPage({ darkMode }) {
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [selectedDuid, setSelectedDuid] = useState(null);
  const [selectedDate, setSelectedDate] = useState(() => {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    return yesterday.toISOString().split('T')[0];
  });
  const [bidData, setBidData] = useState(null);
  const [priceBands, setPriceBands] = useState([]);
  const [dispatchData, setDispatchData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);

  const searchTimerRef = useRef(null);
  const dropdownRef = useRef(null);

  // Close dropdown on outside click
  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Debounced DUID search
  const handleSearchChange = useCallback((e) => {
    const query = e.target.value;
    setSearchQuery(query);

    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);

    if (query.length < 2) {
      setSearchResults([]);
      setShowDropdown(false);
      return;
    }

    searchTimerRef.current = setTimeout(async () => {
      setSearchLoading(true);
      try {
        const response = await api.get(`/api/duids/search?q=${encodeURIComponent(query)}`);
        setSearchResults(response.data.results || []);
        setShowDropdown(true);
      } catch (error) {
        console.error('Error searching DUIDs:', error);
        setSearchResults([]);
      } finally {
        setSearchLoading(false);
      }
    }, 300);
  }, []);

  // Select a DUID from dropdown
  const handleSelectDuid = useCallback((result) => {
    setSelectedDuid(result);
    setSearchQuery(result.duid);
    setShowDropdown(false);
  }, []);

  // Fetch bid data when DUID and date are set
  const fetchBidData = useCallback(async () => {
    if (!selectedDuid || !selectedDate) return;

    setLoading(true);
    setBidData(null);
    setDispatchData([]);

    try {
      // Fetch bid data and dispatch data in parallel
      const [bidResponse, dispatchResponse] = await Promise.all([
        api.get(`/api/bids/${selectedDuid.duid}?date=${selectedDate}`),
        api.get(`/api/dispatch/range?start_date=${selectedDate}T00:00:00&end_date=${selectedDate}T23:59:59&duid=${selectedDuid.duid}`)
          .catch(() => ({ data: { data: [] } }))
      ]);

      setBidData(bidResponse.data.data || []);
      setPriceBands(bidResponse.data.price_bands || []);
      setDispatchData(dispatchResponse.data.data || []);
    } catch (error) {
      console.error('Error fetching bid data:', error);
      setBidData([]);
    } finally {
      setLoading(false);
    }
  }, [selectedDuid, selectedDate]);

  useEffect(() => {
    fetchBidData();
  }, [fetchBidData]);

  // Build sorted band indices (cheapest to most expensive)
  const sortedBands = useMemo(() => {
    if (!priceBands || priceBands.length === 0) return [];

    return Array.from({ length: 10 }, (_, i) => ({
      index: i + 1,
      price: priceBands[i]
    }))
      .filter(b => b.price != null)
      .sort((a, b) => a.price - b.price);
  }, [priceBands]);

  // Build Plotly chart data
  const chartData = useMemo(() => {
    if (!bidData || bidData.length === 0 || sortedBands.length === 0) return [];

    const timestamps = bidData.map(d => new Date(d.settlementdate));

    // Build per-timestamp custom hover text (only non-zero bands)
    const hoverTexts = bidData.map((d, idx) => {
      const time = timestamps[idx];
      const timeStr = `${String(time.getHours()).padStart(2, '0')}:${String(time.getMinutes()).padStart(2, '0')}`;
      const lines = [`<b>${timeStr}</b>`];

      // Show bands from most expensive to cheapest (top to bottom)
      const reversed = [...sortedBands].reverse();
      reversed.forEach((band, i) => {
        const qty = d[`bandavail${band.index}`] || 0;
        if (qty > 0) {
          const colorIdx = sortedBands.length - 1 - i;
          const color = BAND_COLORS[colorIdx] || BAND_COLORS[BAND_COLORS.length - 1];
          lines.push(`<span style="color:${color}">\u25A0</span> Band ${band.index}: ${qty.toFixed(0)} MW @ $${band.price.toFixed(0)}/MWh`);
        }
      });

      // Max avail
      if (d.maxavail != null && d.maxavail > 0) {
        lines.push(`Max Avail: ${d.maxavail.toFixed(0)} MW`);
      }

      return lines.join('<br>');
    });

    // Stacked bars for each band (cheapest at bottom), hover disabled
    const traces = sortedBands.map((band, colorIdx) => ({
      x: timestamps,
      y: bidData.map(d => d[`bandavail${band.index}`] || 0),
      type: 'bar',
      name: `Band ${band.index} ($${band.price.toFixed(0)})`,
      marker: { color: BAND_COLORS[colorIdx] || BAND_COLORS[BAND_COLORS.length - 1] },
      hoverinfo: 'skip'
    }));

    // Invisible scatter trace that carries the combined hover tooltip
    traces.push({
      x: timestamps,
      y: bidData.map(d => {
        let total = 0;
        sortedBands.forEach(band => { total += (d[`bandavail${band.index}`] || 0); });
        return total;
      }),
      type: 'scatter',
      mode: 'markers',
      marker: { size: 0.1, opacity: 0 },
      text: hoverTexts,
      hovertemplate: '%{text}<extra></extra>',
      showlegend: false
    });

    // MAXAVAIL line
    const maxAvailValues = bidData.map(d => d.maxavail);
    if (maxAvailValues.some(v => v != null && v > 0)) {
      traces.push({
        x: timestamps,
        y: maxAvailValues,
        type: 'scatter',
        mode: 'lines',
        name: 'Max Avail',
        line: { color: darkMode ? '#a78bfa' : '#7c3aed', width: 2, dash: 'dash' },
        hoverinfo: 'skip'
      });
    }

    // Actual dispatch output overlay
    if (dispatchData.length > 0) {
      traces.push({
        x: dispatchData.map(d => new Date(d.settlementdate)),
        y: dispatchData.map(d => d.scadavalue),
        type: 'scatter',
        mode: 'lines',
        name: 'Actual Output',
        line: { color: darkMode ? '#f5f5f5' : '#111', width: 2.5 },
        hovertemplate: 'Actual: %{y:.0f} MW<extra></extra>'
      });
    }

    // Minimum load line
    if (bidData.length > 0 && bidData[0].minimumload != null && bidData[0].minimumload > 0) {
      const minLoad = bidData[0].minimumload;
      traces.push({
        x: [timestamps[0], timestamps[timestamps.length - 1]],
        y: [minLoad, minLoad],
        type: 'scatter',
        mode: 'lines',
        name: `Min Load (${minLoad.toFixed(0)} MW)`,
        line: { color: '#f59e0b', width: 1.5, dash: 'dot' },
        hoverinfo: 'skip'
      });
    }

    return traces;
  }, [bidData, sortedBands, dispatchData, darkMode]);

  const chartLayout = useMemo(() => ({
    barmode: 'stack',
    bargap: 0,
    xaxis: {
      title: { text: 'Time (AEST)', font: { size: 12 } },
      tickformat: '%H:%M',
      gridcolor: darkMode ? '#374151' : '#f3f4f6',
      tickfont: { color: darkMode ? '#9ca3af' : '#6b7280' },
      zeroline: false
    },
    yaxis: {
      title: { text: 'MW', font: { size: 12 } },
      gridcolor: darkMode ? '#374151' : '#f3f4f6',
      tickfont: { color: darkMode ? '#9ca3af' : '#6b7280' },
      zeroline: false,
      rangemode: 'tozero'
    },
    plot_bgcolor: 'transparent',
    paper_bgcolor: 'transparent',
    font: { color: darkMode ? '#f5f5f5' : '#333' },
    hoverlabel: {
      bgcolor: darkMode ? '#1f1f1f' : '#fff',
      bordercolor: darkMode ? '#555' : '#ddd',
      font: { color: darkMode ? '#f5f5f5' : '#333', size: 12 }
    },
    legend: {
      orientation: 'h',
      y: -0.2,
      x: 0.5,
      xanchor: 'center',
      font: { size: 11 }
    },
    margin: { l: 60, r: 20, t: 10, b: 80 },
    hovermode: 'x unified'
  }), [darkMode]);

  const plotConfig = useMemo(() => ({
    displayModeBar: 'hover',
    displaylogo: false,
    modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d', 'autoScale2d'],
    scrollZoom: false
  }), []);

  return (
    <div className={`bid-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="bid-header">
        <h1 className="bid-title">Bid Analysis</h1>
        <p className="bid-subtitle">Generator bid band structure by dispatch interval</p>
      </div>

      <div className="bid-controls">
        <div className="duid-search-wrapper" ref={dropdownRef}>
          <input
            type="text"
            className="duid-search-input"
            placeholder="Search DUID or station name..."
            value={searchQuery}
            onChange={handleSearchChange}
            onFocus={() => searchResults.length > 0 && setShowDropdown(true)}
          />
          {showDropdown && searchResults.length > 0 && (
            <div className="duid-dropdown">
              {searchResults.map((result) => (
                <div
                  key={result.duid}
                  className="duid-option"
                  onClick={() => handleSelectDuid(result)}
                >
                  <div className="duid-option-name">{result.duid}</div>
                  <div className="duid-option-detail">
                    {result.station_name} &middot; {result.region} &middot; {result.fuel_source} &middot; {result.capacity_mw?.toFixed(0)} MW
                  </div>
                </div>
              ))}
            </div>
          )}
          {searchLoading && (
            <div className="duid-dropdown">
              <div className="duid-option" style={{ textAlign: 'center', color: '#999' }}>
                Searching...
              </div>
            </div>
          )}
        </div>

        <div className="bid-date-picker">
          <label>Date:</label>
          <input
            type="date"
            className="bid-date-input"
            value={selectedDate}
            onChange={(e) => setSelectedDate(e.target.value)}
          />
        </div>
      </div>

      {selectedDuid && (
        <div className="generator-info-card">
          <div className="gen-info-item">
            <span className="gen-info-label">DUID</span>
            <span className="gen-info-value">{selectedDuid.duid}</span>
          </div>
          <div className="gen-info-item">
            <span className="gen-info-label">Station</span>
            <span className="gen-info-value">{selectedDuid.station_name || '—'}</span>
          </div>
          <div className="gen-info-item">
            <span className="gen-info-label">Region</span>
            <span className="gen-info-value">{selectedDuid.region || '—'}</span>
          </div>
          <div className="gen-info-item">
            <span className="gen-info-label">Fuel</span>
            <span className="gen-info-value">{selectedDuid.fuel_source || '—'}</span>
          </div>
          <div className="gen-info-item">
            <span className="gen-info-label">Capacity</span>
            <span className="gen-info-value">{selectedDuid.capacity_mw ? `${selectedDuid.capacity_mw.toFixed(0)} MW` : '—'}</span>
          </div>
        </div>
      )}

      {loading && (
        <div className="bid-loading">
          <div className="spinner"></div>
          <p>Loading bid data...</p>
        </div>
      )}

      {!loading && !selectedDuid && (
        <div className="bid-empty">
          <p>Search for a generator DUID above to view its bid band structure.</p>
        </div>
      )}

      {!loading && selectedDuid && bidData && bidData.length === 0 && (
        <div className="bid-empty">
          <p>No bid data available for {selectedDuid.duid} on {selectedDate}.</p>
          <p style={{ fontSize: '0.85rem', marginTop: '8px' }}>
            Bid data may not yet be ingested for this date. Try a recent date or trigger a backfill.
          </p>
        </div>
      )}

      {!loading && bidData && bidData.length > 0 && (
        <div className="bid-chart-section">
          <h3 className="bid-chart-title">
            Bid Bands — {selectedDuid.duid} — {selectedDate}
          </h3>
          <Plot
            data={chartData}
            layout={chartLayout}
            style={{ width: '100%', height: '500px' }}
            config={plotConfig}
          />
        </div>
      )}
    </div>
  );
}

export default BidBandPage;
