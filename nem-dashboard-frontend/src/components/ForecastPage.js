import React, { useState, useEffect, useCallback, useRef } from 'react';
import Plot from 'react-plotly.js';
import api from '../api';
import './ForecastPage.css';

const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'];

const REGION_COLORS = {
  NSW1: '#1f77b4',
  VIC1: '#ff7f0e',
  QLD1: '#2ca02c',
  SA1: '#d62728',
  TAS1: '#9467bd',
};

// Hex -> rgba so the area fill can be a translucent tint of the line colour.
const fillFor = (hex, alpha) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
};

function ForecastPage({ darkMode }) {
  const [region, setRegion] = useState('NSW1');
  const [forecast, setForecast] = useState([]);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const [training, setTraining] = useState(false);
  const [trainNote, setTrainNote] = useState(null);
  const pollRef = useRef(null);

  const fetchForecast = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await api.get('/api/forecast/prices', { params: { region } });
      setForecast(response.data.data || []);
      setMeta({ trainedAt: response.data.model_trained_at, count: response.data.count });
    } catch (err) {
      const status = err.response?.status;
      setError(
        status === 503
          ? 'No trained model found. Click “Retrain model”, or run the training CLI on the backend.'
          : err.response?.data?.detail || 'Failed to load forecast.'
      );
      setForecast([]);
      setMeta(null);
    } finally {
      setLoading(false);
    }
  }, [region]);

  useEffect(() => {
    fetchForecast();
  }, [fetchForecast]);

  // Stop polling if the component unmounts mid-train.
  useEffect(() => () => {
    if (pollRef.current) clearInterval(pollRef.current);
  }, []);

  const startPolling = useCallback(() => {
    if (pollRef.current) clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      try {
        const { data } = await api.get('/api/forecast/status');
        if (data.status === 'done') {
          clearInterval(pollRef.current);
          pollRef.current = null;
          setTraining(false);
          const m = data.metrics || {};
          setTrainNote(
            `Done — ${data.n_rows?.toLocaleString() ?? '?'} rows` +
              (m.mae != null ? ` · MAE $${m.mae.toFixed(0)}` : '') +
              (m.spearman != null ? ` · Spearman ${m.spearman.toFixed(2)}` : '')
          );
          fetchForecast(); // refresh chart with the freshly trained model
        } else if (data.status === 'error') {
          clearInterval(pollRef.current);
          pollRef.current = null;
          setTraining(false);
          setTrainNote(`Retrain failed: ${data.error || 'unknown error'}`);
        }
      } catch (e) {
        /* transient — keep polling */
      }
    }, 3000);
  }, [fetchForecast]);

  const handleRetrain = async () => {
    setTraining(true);
    setTrainNote('Retraining on the last year of data… (~1–2 min)');
    try {
      await api.post('/api/forecast/retrain');
    } catch (err) {
      if (err.response?.status !== 409) {
        // 409 just means one is already running — fall through to polling.
        setTraining(false);
        setTrainNote(err.response?.data?.detail || 'Could not start retraining.');
        return;
      }
    }
    startPolling();
  };

  const color = REGION_COLORS[region];

  const plotData = [
    {
      x: forecast.map((d) => new Date(d.interval_datetime)),
      y: forecast.map((d) => d.predicted_price),
      type: 'scatter',
      mode: 'lines',
      name: region,
      line: { color, width: 2, shape: 'hv' },
      fill: 'tozeroy',
      fillcolor: fillFor(color, 0.12),
      hovertemplate: '%{x|%a %d %b %H:%M}<br>$%{y:.0f}/MWh<extra></extra>',
    },
  ];

  const plotLayout = {
    title: {
      text: `${region} — 7-day price forecast (P50)`,
      font: { size: 20, color: darkMode ? '#f5f5f5' : '#333' },
    },
    xaxis: {
      title: 'Settlement interval (30-min)',
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333',
      tickformat: '%a %d %b\n%H:%M',
    },
    yaxis: {
      title: 'Forecast price ($/MWh)',
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333',
      zeroline: true,
      zerolinecolor: darkMode ? '#555' : '#ccc',
    },
    plot_bgcolor: darkMode ? '#1a1a1a' : 'white',
    paper_bgcolor: darkMode ? '#1a1a1a' : 'white',
    font: { color: darkMode ? '#f5f5f5' : '#333' },
    margin: { l: 60, r: 30, t: 60, b: 70 },
    hovermode: 'x unified',
  };

  const formatTrainedAt = (iso) => {
    if (!iso) return null;
    const d = new Date(iso.endsWith('Z') ? iso : `${iso}Z`);
    return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
  };

  return (
    <div className={`forecast-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="forecast-controls">
        <label htmlFor="forecast-region">Region</label>
        <select
          id="forecast-region"
          value={region}
          onChange={(e) => setRegion(e.target.value)}
        >
          {REGIONS.map((r) => (
            <option key={r} value={r}>
              {r}
            </option>
          ))}
        </select>

        <button className="retrain-btn" onClick={handleRetrain} disabled={training}>
          {training ? 'Retraining…' : 'Retrain model'}
        </button>

        {meta && !error && (
          <span className="forecast-meta">
            {meta.count} intervals · P50
            {meta.trainedAt && ` · trained ${formatTrainedAt(meta.trainedAt)}`}
          </span>
        )}
      </div>

      {trainNote && <div className="forecast-trainnote">{trainNote}</div>}

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <p>Generating forecast…</p>
        </div>
      )}

      {!loading && error && (
        <div className="forecast-error">
          <p>{error}</p>
        </div>
      )}

      {!loading && !error && (
        <div className="chart-container">
          <Plot
            data={plotData}
            layout={plotLayout}
            useResizeHandler
            style={{ width: '100%', height: '600px' }}
            config={{
              displayModeBar: true,
              displaylogo: false,
              modeBarButtonsToRemove: [
                'pan2d',
                'lasso2d',
                'select2d',
                'autoScale2d',
                'hoverClosestCartesian',
                'hoverCompareCartesian',
              ],
            }}
          />
        </div>
      )}
    </div>
  );
}

export default ForecastPage;
