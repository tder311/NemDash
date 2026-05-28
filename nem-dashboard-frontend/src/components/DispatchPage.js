import React, { useState, useCallback, useMemo } from 'react';
import Plot from 'react-plotly.js';
import api from '../api';
import './DispatchPage.css';

const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'];

const DEFAULTS = {
  region: 'NSW1',
  powerMw: 100,
  durationH: 2.0,
  rtePct: 85,
  cycleCost: 0,
  cyclic: true,
};

const DT_HOURS = 0.5; // 30-minute intervals

function fmtMoney(v) {
  if (v == null || Number.isNaN(v)) return '–';
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `$${(v / 1_000).toFixed(1)}k`;
  return `$${v.toFixed(0)}`;
}

function fmtNum(v, digits = 0) {
  if (v == null || Number.isNaN(v)) return '–';
  return v.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function DispatchPage({ darkMode }) {
  const [form, setForm] = useState(DEFAULTS);
  const [applied, setApplied] = useState(null);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

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
          cycle_cost_per_mwh: cfg.cycleCost,
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

  const handleSubmit = (e) => {
    e.preventDefault();
    runOptimisation(form);
  };

  const setField = (key) => (e) => {
    const v = e.target.type === 'checkbox' ? e.target.checked : e.target.value;
    setForm((f) => ({ ...f, [key]: v }));
  };

  const schedule = result?.schedule ?? [];

  // ---- daily roll-up (charge/discharge MWh, weighted prices, revenue) ----
  const daily = useMemo(() => {
    if (!schedule.length) return [];
    const byDay = new Map();
    for (const r of schedule) {
      const d = new Date(r.interval_datetime);
      const key = d.toISOString().slice(0, 10);
      if (!byDay.has(key)) {
        byDay.set(key, {
          key,
          label: d.toLocaleDateString('en-AU', { weekday: 'short', day: '2-digit', month: 'short' }),
          chargeMwh: 0,
          dischargeMwh: 0,
          buyDollars: 0,
          sellDollars: 0,
          revenue: 0,
        });
      }
      const b = byDay.get(key);
      b.chargeMwh += r.charge_mw * DT_HOURS;
      b.dischargeMwh += r.discharge_mw * DT_HOURS;
      b.buyDollars += r.price * r.charge_mw * DT_HOURS;
      b.sellDollars += r.price * r.discharge_mw * DT_HOURS;
      b.revenue += r.revenue;
    }
    return Array.from(byDay.values()).map((b) => ({
      ...b,
      avgBuy: b.chargeMwh > 1e-6 ? b.buyDollars / b.chargeMwh : null,
      avgSell: b.dischargeMwh > 1e-6 ? b.sellDollars / b.dischargeMwh : null,
    }));
  }, [schedule]);

  // ---- chart traces (2 stacked subplots: price on top, net dispatch below) ----
  const x = schedule.map((d) => new Date(d.interval_datetime));
  const yPrice = schedule.map((d) => d.price);
  const yNet = schedule.map((d) => d.net_mw);

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
          hovertemplate: '%{x|%a %d %b %H:%M}<br>Price $%{y:.0f}/MWh<extra></extra>',
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
          hovertemplate: '%{x|%a %d %b %H:%M}<br>Net %{y:.1f} MW<extra></extra>',
        },
      ]
    : [];

  const plotLayout = {
    title: {
      text: result
        ? `${result.region} dispatch — ${fmtMoney(result.total_revenue)} · ${result.n_cycles.toFixed(2)} cycles`
        : 'Dispatch',
      font: { size: 18, color: darkMode ? '#f5f5f5' : '#333' },
    },
    xaxis: {
      anchor: 'y2',
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333',
      tickformat: '%a %d %b\n%H:%M',
    },
    yaxis: {
      title: 'Price ($/MWh)',
      domain: [0.56, 1.0],
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333',
    },
    yaxis2: {
      title: 'Net MW',
      domain: [0, 0.44],
      gridcolor: darkMode ? '#404040' : '#e0e0e0',
      color: darkMode ? '#f5f5f5' : '#333',
      zeroline: true,
      zerolinecolor: darkMode ? '#666' : '#bbb',
    },
    plot_bgcolor: darkMode ? '#1a1a1a' : 'white',
    paper_bgcolor: darkMode ? '#1a1a1a' : 'white',
    font: { color: darkMode ? '#f5f5f5' : '#333' },
    legend: { orientation: 'h', x: 0.5, xanchor: 'center', y: -0.18 },
    margin: { l: 65, r: 30, t: 60, b: 90 },
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
          <input type="number" min="1" step="1" value={form.powerMw} onChange={setField('powerMw')} />
        </label>

        <label>
          Duration (h)
          <input type="number" min="0.5" step="0.5" value={form.durationH} onChange={setField('durationH')} />
        </label>

        <label>
          RTE (%)
          <input type="number" min="50" max="100" step="1" value={form.rtePct} onChange={setField('rtePct')} />
        </label>

        <label>
          Cycle cost ($/MWh)
          <input type="number" min="0" step="1" value={form.cycleCost} onChange={setField('cycleCost')} />
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
          · RTE {applied.rtePct}%
          {Number(applied.cycleCost) > 0 && ` · cycle cost $${applied.cycleCost}/MWh`}
          {' · '}{applied.cyclic ? 'cyclic' : 'free-end'}
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

      {!loading && error && <div className="dispatch-error"><p>{error}</p></div>}

      {!loading && !error && !result && (
        <div className="dispatch-empty">
          Configure your battery above and click <b>Run optimisation</b>.
        </div>
      )}

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <p>Solving LP…</p>
        </div>
      )}

      {!loading && !error && schedule.length > 0 && (
        <>
          <div className="chart-container">
            <Plot
              data={plotData}
              layout={plotLayout}
              useResizeHandler
              style={{ width: '100%', height: '640px' }}
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

          {daily.length > 0 && (
            <div className="daily-table-wrap">
              <h3 className="daily-title">Daily summary</h3>
              <table className="daily-table">
                <thead>
                  <tr>
                    <th>Day</th>
                    <th className="num">Charge (MWh)</th>
                    <th className="num">Discharge (MWh)</th>
                    <th className="num">Avg buy ($/MWh)</th>
                    <th className="num">Avg sell ($/MWh)</th>
                    <th className="num">Revenue ($)</th>
                  </tr>
                </thead>
                <tbody>
                  {daily.map((d) => (
                    <tr key={d.key}>
                      <td>{d.label}</td>
                      <td className="num">{fmtNum(d.chargeMwh, 1)}</td>
                      <td className="num">{fmtNum(d.dischargeMwh, 1)}</td>
                      <td className="num">{d.avgBuy != null ? fmtNum(d.avgBuy, 0) : '—'}</td>
                      <td className="num">{d.avgSell != null ? fmtNum(d.avgSell, 0) : '—'}</td>
                      <td className="num">{fmtNum(d.revenue, 0)}</td>
                    </tr>
                  ))}
                </tbody>
                <tfoot>
                  <tr>
                    <td>Total</td>
                    <td className="num">{fmtNum(daily.reduce((s, d) => s + d.chargeMwh, 0), 1)}</td>
                    <td className="num">{fmtNum(daily.reduce((s, d) => s + d.dischargeMwh, 0), 1)}</td>
                    <td className="num">—</td>
                    <td className="num">—</td>
                    <td className="num">{fmtNum(daily.reduce((s, d) => s + d.revenue, 0), 0)}</td>
                  </tr>
                </tfoot>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default DispatchPage;
