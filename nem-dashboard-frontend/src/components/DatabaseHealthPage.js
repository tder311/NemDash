import React, { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import './DatabaseHealthPage.css';

const TIME_RANGE_OPTIONS = [
  { value: 24, label: '24 Hours' },
  { value: 168, label: '7 Days' },
  { value: 720, label: '30 Days' }
];

function formatNumber(num) {
  if (num === null || num === undefined) return '0';
  return num.toLocaleString();
}

function formatDate(dateStr) {
  if (!dateStr) return 'N/A';
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-AU', {
    day: 'numeric',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function formatShortDate(dateStr) {
  if (!dateStr) return 'N/A';
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-AU', {
    day: 'numeric',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function DatabaseHealthPage({ darkMode, onBack }) {
  const [healthData, setHealthData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [hoursBack, setHoursBack] = useState(168);
  const [expandedTables, setExpandedTables] = useState({});

  const fetchHealthData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await axios.get(`/api/database/health?hours_back=${hoursBack}`);
      setHealthData(response.data);
      setLoading(false);
    } catch (err) {
      console.error('Error fetching database health:', err);
      setError('Failed to fetch database health data');
      setLoading(false);
    }
  }, [hoursBack]);

  useEffect(() => {
    fetchHealthData();
  }, [fetchHealthData]);

  const toggleTableExpand = (tableName) => {
    setExpandedTables(prev => ({
      ...prev,
      [tableName]: !prev[tableName]
    }));
  };

  const getTableGaps = (tableName) => {
    if (!healthData?.gaps) return null;
    return healthData.gaps.find(g => g.table === tableName);
  };

  const getTotalGaps = () => {
    if (!healthData?.gaps) return 0;
    return healthData.gaps.reduce((sum, g) => sum + g.total_gaps, 0);
  };

  const getTotalRecords = () => {
    if (!healthData?.tables) return 0;
    return healthData.tables.reduce((sum, t) => sum + (t.total_records || 0), 0);
  };

  if (loading) {
    return (
      <div className={`database-health-container ${darkMode ? 'dark' : 'light'}`}>
        <div className="loading">
          <div className="spinner"></div>
          <p>Loading database health data...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`database-health-container ${darkMode ? 'dark' : 'light'}`}>
        <div className="health-header">
          <button className="back-button" onClick={onBack}>
            Back
          </button>
          <h1 className="health-title">Database Health</h1>
        </div>
        <div className="error-message">
          <p>{error}</p>
          <button onClick={fetchHealthData} className="retry-button">Retry</button>
        </div>
      </div>
    );
  }

  return (
    <div className={`database-health-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="health-header">
        <button className="back-button" onClick={onBack}>
          Back
        </button>
        <h1 className="health-title">Database Health</h1>
        <div className="last-checked">
          Checked: {healthData?.checked_at ? formatShortDate(healthData.checked_at) : 'N/A'}
        </div>
      </div>

      <div className="time-range-selector">
        <label>Gap Detection Range:</label>
        <div className="time-range-buttons">
          {TIME_RANGE_OPTIONS.map(option => (
            <button
              key={option.value}
              className={`range-button ${hoursBack === option.value ? 'active' : ''}`}
              onClick={() => setHoursBack(option.value)}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      <div className="summary-cards">
        <div className="summary-card">
          <div className="card-label">Total Records</div>
          <div className="card-value">{formatNumber(getTotalRecords())}</div>
        </div>
        <div className="summary-card">
          <div className="card-label">Tables Monitored</div>
          <div className="card-value">{healthData?.tables?.length || 0}</div>
        </div>
        <div className={`summary-card ${getTotalGaps() > 0 ? 'has-gaps' : 'no-gaps'}`}>
          <div className="card-label">Gaps Detected</div>
          <div className="card-value">{formatNumber(getTotalGaps())}</div>
          <div className="card-subtitle">in last {TIME_RANGE_OPTIONS.find(o => o.value === hoursBack)?.label}</div>
        </div>
      </div>

      <div className="section-header">
        <h2>Table Statistics</h2>
      </div>

      <div className="table-stats-grid">
        {healthData?.tables?.map(table => {
          const gaps = getTableGaps(table.table);
          const hasGaps = gaps && gaps.total_gaps > 0;

          return (
            <div key={table.table} className={`table-stat-card ${hasGaps ? 'has-gaps' : ''}`}>
              <div className="table-name">
                {table.table}
                {hasGaps && <span className="gap-badge">{gaps.total_gaps} gaps</span>}
                {!hasGaps && table.expected_interval && <span className="ok-badge">OK</span>}
              </div>
              <div className="table-records">
                <span className="stat-value">{formatNumber(table.total_records)}</span>
                <span className="stat-label">records</span>
              </div>
              <div className="table-date-range">
                {table.earliest_date ? (
                  <>
                    <span className="date-label">From:</span> {formatShortDate(table.earliest_date)}
                    <br />
                    <span className="date-label">To:</span> {formatShortDate(table.latest_date)}
                  </>
                ) : (
                  <span className="no-data">No data</span>
                )}
              </div>
              {table.days_with_data !== null && (
                <div className="table-days">
                  <span className="stat-value">{formatNumber(table.days_with_data)}</span>
                  <span className="stat-label">days with data</span>
                </div>
              )}
              {table.expected_interval && (
                <div className="table-interval">
                  Expected: {table.expected_interval} min intervals
                </div>
              )}
            </div>
          );
        })}
      </div>

      <div className="section-header">
        <h2>Data Gaps</h2>
        <span className="section-subtitle">Last {TIME_RANGE_OPTIONS.find(o => o.value === hoursBack)?.label}</span>
      </div>

      <div className="gaps-section">
        {healthData?.gaps?.map(tableGaps => (
          <div key={tableGaps.table} className="gaps-table-wrapper">
            <div
              className={`gaps-table-header ${expandedTables[tableGaps.table] ? 'expanded' : ''}`}
              onClick={() => toggleTableExpand(tableGaps.table)}
            >
              <span className="expand-icon">{expandedTables[tableGaps.table] ? '▼' : '▶'}</span>
              <span className="gaps-table-name">{tableGaps.table}</span>
              <span className={`gaps-count ${tableGaps.total_gaps === 0 ? 'no-gaps' : 'has-gaps'}`}>
                {tableGaps.total_gaps === 0 ? 'No gaps detected' : `${tableGaps.total_gaps} gap${tableGaps.total_gaps !== 1 ? 's' : ''}`}
              </span>
            </div>
            {expandedTables[tableGaps.table] && tableGaps.gaps.length > 0 && (
              <div className="gaps-list">
                <table className="gaps-detail-table">
                  <thead>
                    <tr>
                      <th>Gap Start</th>
                      <th>Gap End</th>
                      <th>Duration</th>
                      <th>Missing Intervals</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tableGaps.gaps.map((gap, idx) => (
                      <tr key={idx}>
                        <td>{formatShortDate(gap.gap_start)}</td>
                        <td>{formatShortDate(gap.gap_end)}</td>
                        <td>{gap.duration_minutes} min</td>
                        <td>{gap.missing_intervals}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            {expandedTables[tableGaps.table] && tableGaps.gaps.length === 0 && (
              <div className="no-gaps-message">
                No gaps detected in the selected time range.
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export default DatabaseHealthPage;
