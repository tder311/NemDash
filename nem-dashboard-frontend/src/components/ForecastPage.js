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

// Display y-range guards ($/MWh): cap hides the VOLL spikes that would flatten
// the chart; floor keeps moderate negative prices visible.
const CAP_MIN = 300;
const CAP_MAX = 2000;
const FLOOR_MIN = -100;

const fillFor = (hex, alpha) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
};

const arrMin = (a) => a.reduce((m, v) => (v < m ? v : m), Infinity);
const arrMax = (a) => a.reduce((m, v) => (v > m ? v : m), -Infinity);

const quantile = (arr, q) => {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const pos = (s.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  return s[base + 1] !== undefined ? s[base] + rest * (s[base + 1] - s[base]) : s[base];
};

function ForecastPage({ darkMode }) {
  const [region, setRegion] = useState('NSW1');
  const [forecast, setForecast] = useState([]);
  const [pd, setPd] = useState([]);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const [training, setTraining] = useState(false);
  const [trainNote, setTrainNote] = useState(null);
  const pollRef = useRef(null);

  const fetchAll = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const fc = await api.get('/api/forecast/prices', { params: { region } });
      setForecast(fc.data.data || []);
      setMeta({ trainedAt: fc.data.model_trained_at, count: fc.data.count });
    } catch (err) {
      const status = err.response?.status;
      setError(
        status === 503
          ? 'No trained model found. Click “Retrain model”, or run the training CLI on the backend.'
          : err.response?.data?.detail || 'Failed to load forecast.'
      );
      setForecast([]);
      setMeta(null);
    }
    // Pre-dispatch overlay is best-effort — absence shouldn't break the page.
    try {
      const r = await api.get('/api/predispatch/prices', { params: { region } });
      setPd(r.data.data || []);
    } catch {
      setPd([]);
    }
    setLoading(false);
  }, [region]);

  useEffect(() => {
    fetchAll();
  }, [fetchAll]);

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
          fetchAll();
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
  }, [fetchAll]);

  const handleRetrain = async () => {
    setTraining(true);
    setTrainNote('Retraining on the last year of data… (~1–2 min)');
    try {
      await api.post('/api/forecast/retrain');
    } catch (err) {
      if (err.response?.status !== 409) {
        setTraining(false);
        setTrainNote(err.response?.data?.detail || 'Could not start retraining.');
        return;
      }
    }
    startPolling();
  };

  const color = REGION_COLORS[region];

  // --- assemble traces + clamp/clip the y-axis around VOLL spikes ---
  const fx = forecast.map((d) => new Date(d.interval_datetime));
  const fy = forecast.map((d) => d.predicted_price);
  const px = pd.map((d) => new Date(d.interval_datetime));
  const pyTrue = pd.map((d) => d.rrp);

  const allVals = [...fy, ...pyTrue];
  const cap = allVals.length
    ? Math.min(CAP_MAX, Math.max(CAP_MIN, quantile(allVals, 0.98) * 1.15))
    : CAP_MIN;
  const yMin = allVals.length ? Math.max(FLOOR_MIN, Math.min(0, arrMin(allVals))) : FLOOR_MIN;
  const clamp = (v) => Math.max(yMin, Math.min(cap, v));

  const plotData = [
    {
      x: fx,
      y: fy.map(clamp),
      customdata: fy,
      type: 'scatter',
      mode: 'lines',
      name: 'Model forecast',
      line: { color, width: 2, shape: 'hv' },
      fill: 'tozeroy',
      fillcolor: fillFor(color, 0.1),
      hovertemplate: '%{x|%a %d %b %H:%M}<br>Model: $%{customdata:.0f}/MWh<extra></extra>',
    },
  ];

  if (pd.length) {
    plotData.push({
      x: px,
      y: pyTrue.map(clamp),
      customdata: pyTrue,
      type: 'scatter',
      mode: 'lines',
      name: 'AEMO pre-dispatch',
      line: { color: darkMode ? '#cfcfcf' : '#444', width: 2, dash: 'dot', shape: 'hv' },
      hovertemplate: '%{x|%a %d %b %H:%M}<br>Pre-dispatch: $%{customdata:.0f}/MWh<extra></extra>',
    });

    // Markers for pre-dispatch intervals clipped at the cap (e.g. VOLL).
    const clipIdx = pyTrue.map((v, i) => (v > cap ? i : -1)).filter((i) => i >= 0);
    if (clipIdx.length) {
      plotData.push({
        x: clipIdx.map((i) => px[i]),
        y: clipIdx.map(() => cap),
        customdata: clipIdx.map((i) => pyTrue[i]),
        type: 'scatter',
        mode: 'markers',
        name: 'PD spike (clipped)',
        marker: { color: '#d62728', symbol: 'triangle-up', size: 10 },
        hovertemplate: '%{x|%a %d %b %H:%M}<br>Pre-dispatch: $%{customdata:.0f}/MWh (clipped)<extra></extra>',
      });
    }
  }

  const plotLayout = {
    title: {
      text: `${region} — 7-day price forecast vs AEMO pre-dispatch`,
      font: { size: 19, color: darkMode ? '#f5f5f5' : '#333' },
    },
    xaxis: {
      title: 'Settlement interval (30-min)',
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333',
      tickformat: '%a %d %b\n%H:%M',
    },
    yaxis: {
      title: 'Price ($/MWh)',
      range: [yMin, cap],
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333',
      zeroline: true,
      zerolinecolor: darkMode ? '#555' : '#ccc',
    },
    plot_bgcolor: darkMode ? '#1a1a1a' : 'white',
    paper_bgcolor: darkMode ? '#1a1a1a' : 'white',
    font: { color: darkMode ? '#f5f5f5' : '#333' },
    legend: { orientation: 'h', x: 0.5, xanchor: 'center', y: -0.18 },
    margin: { l: 60, r: 30, t: 60, b: 90 },
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
            {` · y-axis capped at $${cap.toFixed(0)}`}
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
