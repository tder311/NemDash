import React, { useState, useEffect, useCallback, useRef } from 'react';
import Plot from 'react-plotly.js';
import api from '../api';
import './DispatchPage.css';

const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'];

const DEFAULTS = {
  region: 'NSW1',
  powerMw: 100,
  durationH: 2.0,
  rtePct: 85,
  cyclic: true,
};

function fmtMoney(v) {
  if (v == null || Number.isNaN(v)) return '–';
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `$${(v / 1_000).toFixed(1)}k`;
  return `$${v.toFixed(0)}`;
}

function DispatchPage({ darkMode }) {
  // Form state (controlled inputs)
  const [form, setForm] = useState(DEFAULTS);
  // Last-applied parameters that are actually shown in the chart
  const [applied, setApplied] = useState(null);

  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const firstLoad = useRef(true);

  const runOptimisation = useCallback(async (cfg) => {
    setLoading(true);
    setError(null);
    try {
      const energyMwh = cfg.powerMw * cfg.durationH;
      const r = await api.get('/api/optimise/dispatch', {
        params: {
          region: cfg.region,
          power_mw: cfg.powerMw,
          energy_mwh: energyMwh,
          eff_rt: cfg.rtePct / 100,
          cyclic: cfg.cyclic,
        },
      });
      setResult(r.data);
      setApplied({ ...cfg, energyMwh });
    } catch (err) {
      const status = err.response?.status;
      setError(
        status === 503
          ? err.response?.data?.detail ||
              'Model or forward PASA data not available. Train the model and ensure ingestion has fresh PASA.'
          : err.response?.data?.detail || 'Failed to run optimisation.'
      );
      setResult(null);
    } finally {
      setLoading(false);
    }
  }, []);

  // Auto-run once on mount with default config so the page isn't blank.
  useEffect(() => {
    if (firstLoad.current) {
      firstLoad.current = false;
      runOptimisation(DEFAULTS);
    }
  }, [runOptimisation]);

  const handleSubmit = (e) => {
    e.preventDefault();
    runOptimisation(form);
  };

  const setField = (key) => (e) => {
    const v = e.target.type === 'checkbox' ? e.target.checked : e.target.value;
    setForm((f) => ({ ...f, [key]: v }));
  };

  // ---- chart traces ----
  const schedule = result?.schedule ?? [];
  const x = schedule.map((d) => new Date(d.interval_datetime));
  const yPrice = schedule.map((d) => d.price);
  const yNet = schedule.map((d) => d.net_mw);
  const ySoc = schedule.map((d) => d.soc_mwh);

  const plotData = schedule.length
    ? [
        {
          x,
          y: yPrice,
          name: 'Forecast price',
          type: 'scatter',
          mode: 'lines',
          line: { color: '#1f77b4', width: 2, shape: 'hv' },
          yaxis: 'y',
          hovertemplate: '%{x|%a %d %b %H:%M}<br>Price: $%{y:.0f}/MWh<extra></extra>',
        },
        {
          x,
          y: yNet,
          name: 'Net dispatch (+ discharge / − charge)',
          type: 'scatter',
          mode: 'lines',
          line: { color: '#2ca02c', width: 1.5, shape: 'hv' },
          fill: 'tozeroy',
          fillcolor: 'rgba(44, 160, 44, 0.18)',
          yaxis: 'y2',
          hovertemplate: '%{x|%a %d %b %H:%M}<br>Net: %{y:.1f} MW<extra></extra>',
        },
        {
          x,
          y: ySoc,
          name: 'SOC',
          type: 'scatter',
          mode: 'lines',
          line: { color: darkMode ? '#cfcfcf' : '#666', width: 1.5, dash: 'dot' },
          yaxis: 'y3',
          hovertemplate: '%{x|%a %d %b %H:%M}<br>SOC: %{y:.0f} MWh<extra></extra>',
        },
      ]
    : [];

  const plotLayout = {
    title: {
      text: result
        ? `${result.region} dispatch — ${fmtMoney(result.total_revenue)} over ${result.count} intervals · ${result.n_cycles.toFixed(2)} cycles`
        : 'Dispatch',
      font: { size: 18, color: darkMode ? '#f5f5f5' : '#333' },
    },
    xaxis: {
      title: 'Settlement interval (30-min)',
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333',
      tickformat: '%a %d %b\n%H:%M',
      domain: [0, 0.92],
    },
    yaxis: {
      title: 'Price ($/MWh)',
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333',
      side: 'left',
    },
    yaxis2: {
      title: 'Net MW',
      overlaying: 'y',
      side: 'right',
      color: darkMode ? '#f5f5f5' : '#333',
      zeroline: true,
      zerolinecolor: darkMode ? '#666' : '#bbb',
      showgrid: false,
    },
    yaxis3: {
      title: 'SOC (MWh)',
      overlaying: 'y',
      side: 'right',
      position: 1.0,
      anchor: 'free',
      color: darkMode ? '#cfcfcf' : '#666',
      showgrid: false,
    },
    plot_bgcolor: darkMode ? '#1a1a1a' : 'white',
    paper_bgcolor: darkMode ? '#1a1a1a' : 'white',
    font: { color: darkMode ? '#f5f5f5' : '#333' },
    legend: { orientation: 'h', x: 0.5, xanchor: 'center', y: -0.18 },
    margin: { l: 60, r: 90, t: 60, b: 90 },
    hovermode: 'x unified',
  };

  return (
    <div className={`dispatch-container ${darkMode ? 'dark' : 'light'}`}>
      <form className="dispatch-controls" onSubmit={handleSubmit}>
        <label>
          Region
          <select value={form.region} onChange={setField('region')}>
            {REGIONS.map((r) => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </label>

        <label>
          Power (MW)
          <input
            type="number" min="1" step="1" value={form.powerMw}
            onChange={setField('powerMw')}
          />
        </label>

        <label>
          Duration (h)
          <input
            type="number" min="0.5" step="0.5" value={form.durationH}
            onChange={setField('durationH')}
          />
        </label>

        <label>
          RTE (%)
          <input
            type="number" min="50" max="100" step="1" value={form.rtePct}
            onChange={setField('rtePct')}
          />
        </label>

        <label className="dispatch-checkbox">
          <input type="checkbox" checked={form.cyclic} onChange={setField('cyclic')} />
          Cyclic SOC
        </label>

        <button type="submit" className="dispatch-btn" disabled={loading}>
          {loading ? 'Optimising…' : 'Run optimisation'}
        </button>
      </form>

      {applied && (
        <div className="dispatch-summary">
          {applied.powerMw} MW · {applied.durationH}h ({applied.energyMwh} MWh)
          · RTE {applied.rtePct}% · {applied.cyclic ? 'cyclic' : 'free-end'}
          {result && (
            <>
              <span className="sep">|</span>
              Status: <b>{result.solver_status}</b>
              <span className="sep">|</span>
              Revenue: <b>{fmtMoney(result.total_revenue)}</b>
              <span className="sep">|</span>
              Cycles: <b>{result.n_cycles.toFixed(2)}</b>
            </>
          )}
        </div>
      )}

      {!loading && error && (
        <div className="dispatch-error"><p>{error}</p></div>
      )}

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <p>Solving LP…</p>
        </div>
      )}

      {!loading && !error && schedule.length > 0 && (
        <div className="chart-container">
          <Plot
            data={plotData}
            layout={plotLayout}
            useResizeHandler
            style={{ width: '100%', height: '620px' }}
            config={{
              displayModeBar: true,
              displaylogo: false,
              modeBarButtonsToRemove: [
                'pan2d', 'lasso2d', 'select2d', 'autoScale2d',
                'hoverClosestCartesian', 'hoverCompareCartesian',
              ],
            }}
          />
        </div>
      )}
    </div>
  );
}

export default DispatchPage;
