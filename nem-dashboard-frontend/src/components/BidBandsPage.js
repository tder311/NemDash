import React, { useState, useCallback } from 'react';
import api from '../api';
import './BidBandsPage.css';

const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'];

const DEFAULTS = {
  region: 'NSW1',
  powerMw: 100,
  durationH: 2.0,
  rtePct: 85,
  cycleCost: 0,
  cyclic: true,
  dayOffset: 0,
  gridMode: 'kinks',
};

function fmtMw(mw) {
  if (mw == null || Number.isNaN(mw) || Math.abs(mw) < 0.05) return '';
  return mw < 10 ? mw.toFixed(1) : mw.toFixed(0);
}

function fmtPriceBand(p) {
  if (p == null) return '–';
  if (Math.abs(p) >= 1000) return `$${(p / 1000).toFixed(p % 1000 === 0 ? 0 : 1)}k`;
  return `$${p}`;
}

function fmtTime(iso) {
  const d = new Date(iso);
  return d.toLocaleTimeString('en-AU', { hour: '2-digit', minute: '2-digit', hour12: false });
}

function fmtDateLabel(iso) {
  const d = new Date(iso);
  return d.toLocaleDateString('en-AU', { weekday: 'short', day: '2-digit', month: 'short' });
}

function BidBandsPage({ darkMode }) {
  const [form, setForm] = useState(DEFAULTS);
  const [applied, setApplied] = useState(null);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const runCompute = useCallback(async (cfg) => {
    setLoading(true);
    setError(null);
    try {
      const energyMwh = cfg.powerMw * cfg.durationH;
      const r = await api.get('/api/bid-bands', {
        params: {
          region: cfg.region,
          power_mw: cfg.powerMw,
          energy_mwh: energyMwh,
          eff_rt: cfg.rtePct / 100,
          cycle_cost_per_mwh: cfg.cycleCost,
          cyclic: cfg.cyclic,
          day_offset: cfg.dayOffset,
          grid_mode: cfg.gridMode,
        },
      });
      setResult(r.data);
      setApplied({ ...cfg, energyMwh });
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to compute bid bands.');
      setResult(null);
    } finally {
      setLoading(false);
    }
  }, []);

  const handleSubmit = (e) => {
    e.preventDefault();
    runCompute(form);
  };

  const setField = (key) => (e) => {
    const v = e.target.type === 'checkbox' ? e.target.checked : e.target.value;
    setForm((f) => ({ ...f, [key]: v }));
  };

  const curves = result?.curves ?? [];
  const priceGrid = result?.price_grid ?? [];
  const pMax = Number(applied?.powerMw ?? DEFAULTS.powerMw);

  // Background tint scaled to power capacity for the heatmap cells.
  const discCell = (mw) => {
    if (!mw || mw < 0.05) return {};
    const alpha = Math.min(1, mw / pMax) * 0.65 + 0.1;
    return { backgroundColor: `rgba(46, 160, 67, ${alpha})`, color: '#fff', fontWeight: 600 };
  };
  const chgCell = (mw) => {
    if (!mw || mw < 0.05) return {};
    const alpha = Math.min(1, mw / pMax) * 0.65 + 0.1;
    return { backgroundColor: `rgba(207, 92, 54, ${alpha})`, color: '#fff', fontWeight: 600 };
  };

  // Headline numbers per side
  const totals = curves.reduce(
    (acc, c) => {
      const sumDis = (c.discharge_tranches || []).reduce((s, v) => s + v, 0);
      const sumCh = (c.charge_tranches || []).reduce((s, v) => s + v, 0);
      acc.discMwh += sumDis * 0.5;
      acc.chgMwh += sumCh * 0.5;
      return acc;
    },
    { discMwh: 0, chgMwh: 0 }
  );

  return (
    <div className={`bidbands-container ${darkMode ? 'dark' : 'light'}`}>
      <form className="bidbands-controls" onSubmit={handleSubmit}>
        <label>
          Region
          <select value={form.region} onChange={setField('region')}>
            {REGIONS.map((r) => <option key={r} value={r}>{r}</option>)}
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
        <label className="bidbands-checkbox">
          <input type="checkbox" checked={form.cyclic} onChange={setField('cyclic')} />
          Cyclic SOC
        </label>
        <label>
          Day
          <select value={form.dayOffset} onChange={setField('dayOffset')}>
            {[0, 1, 2, 3, 4, 5, 6].map((d) => (
              <option key={d} value={d}>Day {d + 1}</option>
            ))}
          </select>
        </label>
        <label>
          Grid
          <select value={form.gridMode} onChange={setField('gridMode')}>
            <option value="kinks">Kink-derived</option>
            <option value="static">Static (default)</option>
          </select>
        </label>
        <button type="submit" className="bidbands-btn" disabled={loading}>
          {loading ? 'Solving…' : 'Compute'}
        </button>
      </form>

      {applied && result && (
        <div className="bidbands-summary">
          {applied.powerMw} MW · {applied.durationH}h ({applied.energyMwh} MWh) · RTE {applied.rtePct}%
          {Number(applied.cycleCost) > 0 && ` · cycle cost $${applied.cycleCost}/MWh`}
          · {applied.cyclic ? 'cyclic' : 'free-end'}
          <span className="sep">|</span>
          {result.curves.length > 0 && (
            <>
              Day: <b>{fmtDateLabel(result.curves[0].interval_datetime)}</b>
              <span className="sep">|</span>
            </>
          )}
          Solves: <b>{result.n_lp_solves}</b>
          <span className="sep">|</span>
          Total: <b>↑{totals.discMwh.toFixed(0)} MWh</b> / <b>↓{totals.chgMwh.toFixed(0)} MWh</b>
        </div>
      )}

      {!loading && error && <div className="bidbands-error"><p>{error}</p></div>}

      {!loading && !error && !result && (
        <div className="bidbands-empty">
          Configure your battery above and click <b>Compute</b>. Heads-up: the LP
          solves ~480 LPs to build a day’s bid stack — it takes ~15-20s.
        </div>
      )}

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <p>Solving ~{48 * (priceGrid.length || 10)} LPs…</p>
        </div>
      )}

      {!loading && !error && curves.length > 0 && (
        <>
          <h3 className="bidbands-title">Discharge — offer stack (BANDAVAIL, MW)</h3>
          <BidTable
            curves={curves}
            priceGrid={priceGrid}
            tranchesKey="discharge_tranches"
            cellStyle={discCell}
          />

          <h3 className="bidbands-title">Charge — load stack (BANDAVAIL, MW)</h3>
          <BidTable
            curves={curves}
            priceGrid={priceGrid}
            tranchesKey="charge_tranches"
            cellStyle={chgCell}
          />
        </>
      )}
    </div>
  );
}

function BidTable({ curves, priceGrid, tranchesKey, cellStyle }) {
  return (
    <div className="bidbands-table-wrap">
      <table className="bidbands-table">
        <thead>
          <tr>
            <th className="sticky-col">Interval</th>
            <th className="num price-col">Fcst $</th>
            {priceGrid.map((p, i) => (
              <th key={i} className="num band-col">{fmtPriceBand(p)}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {curves.map((c) => {
            const tranches = c[tranchesKey] || [];
            return (
              <tr key={c.interval_datetime}>
                <td className="sticky-col mono">{fmtTime(c.interval_datetime)}</td>
                <td className="num mono dim">{c.forecast_price?.toFixed(0)}</td>
                {tranches.map((mw, i) => (
                  <td key={i} className="num mono" style={cellStyle(mw)}>
                    {fmtMw(mw)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default BidBandsPage;
