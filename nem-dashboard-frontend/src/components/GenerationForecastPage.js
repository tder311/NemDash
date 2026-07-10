import React, { useState, useEffect, useCallback } from 'react';
import Plot from 'react-plotly.js';
import api from '../api';
import './GenerationForecastPage.css';

const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'];
const FLEET_COLORS = { Wind: '#2ca02c', Solar: '#ff7f0e' };

// Builds z (utilisation = mw/capacity) and hover customdata over the union of interval
// timestamps across all units, so a gap in one unit's series is a blank cell, not interpolated.
function buildUnitHeatmap(units) {
  const allTimes = Array.from(
    new Set(units.flatMap((u) => u.series.map((p) => p.interval_datetime)))
  ).sort();
  const timeIndex = new Map(allTimes.map((t, i) => [t, i]));

  const z = units.map(() => allTimes.map(() => null));
  const customdata = units.map(() => allTimes.map(() => [null, null]));

  units.forEach((u, row) => {
    u.series.forEach((p) => {
      const col = timeIndex.get(p.interval_datetime);
      z[row][col] = u.capacity_mw > 0 ? p.mw / u.capacity_mw : null;
      customdata[row][col] = [p.mw, p.quality];
    });
  });

  return {
    x: allTimes.map((t) => new Date(t)),
    y: units.map((u) => `${u.station_name} (${u.duid}) · ${u.capacity_mw.toFixed(0)} MW`),
    z,
    customdata,
  };
}

// Builds the two-line (Wind/Solar) MW-sum figure from whichever fleets are present.
function buildFleetFigure(fleets, darkMode) {
  const axisColor = darkMode ? '#f5f5f5' : '#333';
  const gridColor = darkMode ? '#404040' : '#e0e0e0';

  const data = fleets.map((f) => ({
    x: f.series.map((p) => new Date(p.interval_datetime)),
    y: f.series.map((p) => p.mw_sum),
    customdata: f.series.map((p) => [p.n_units, p.capacity_inferable]),
    type: 'scatter',
    mode: 'lines',
    name: f.fuel_source,
    line: { color: FLEET_COLORS[f.fuel_source], width: 2 },
    hovertemplate:
      `${f.fuel_source}<br>%{x|%a %d %b %H:%M}<br>%{y:.0f} MW` +
      '<br>%{customdata[0]} units inferable<extra></extra>',
  }));

  return {
    data,
    layout: {
      plot_bgcolor: darkMode ? '#1a1a1a' : 'white',
      paper_bgcolor: darkMode ? '#1a1a1a' : 'white',
      font: { color: axisColor },
      margin: { l: 60, r: 20, t: 20, b: 40 },
      height: 320,
      legend: { orientation: 'h', y: -0.2 },
      xaxis: { gridcolor: gridColor, color: axisColor, tickformat: '%a %H:%M' },
      yaxis: { title: 'MW', gridcolor: gridColor, color: axisColor },
    },
  };
}

// "n of m units, ~z% of capacity inferable" using the fleet's median interval as a representative snapshot.
function coverageSubtitle(fleet) {
  if (!fleet || fleet.series.length === 0) return null;
  const mid = fleet.series[Math.floor(fleet.series.length / 2)];
  const pct = fleet.capacity_total > 0 ? Math.round((mid.capacity_inferable / fleet.capacity_total) * 100) : 0;
  return `${mid.n_units} of ${fleet.n_units_total} units · ~${pct}% of capacity inferable`;
}

const EMPTY_DATA = { run_datetime: null, units: [], fleets: [], message: '' };

function GenerationForecastPage({ darkMode }) {
  const [region, setRegion] = useState('NSW1');
  const [data, setData] = useState(EMPTY_DATA);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchGenerationForecast = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const { data: resp } = await api.get('/api/network/generation-forecast', { params: { region } });
      setData(resp);
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to load generation forecast.');
      setData(EMPTY_DATA);
    }
    setLoading(false);
  }, [region]);

  useEffect(() => {
    fetchGenerationForecast();
  }, [fetchGenerationForecast]);

  const byFuel = (fuel) =>
    data.units.filter((u) => u.fuel_source === fuel).slice().sort((a, b) => b.capacity_mw - a.capacity_mw);

  const coalUnits = byFuel('Coal');
  const gasUnits = byFuel('Gas');
  const batteryUnits = byFuel('Battery');

  const coalHeatmap = coalUnits.length ? buildUnitHeatmap(coalUnits) : null;
  const gasHeatmap = gasUnits.length ? buildUnitHeatmap(gasUnits) : null;
  const batteryHeatmap = batteryUnits.length ? buildUnitHeatmap(batteryUnits) : null;

  const windFleet = data.fleets.find((f) => f.fuel_source === 'Wind') || null;
  const solarFleet = data.fleets.find((f) => f.fuel_source === 'Solar') || null;
  const presentFleets = [windFleet, solarFleet].filter(Boolean);
  const fleetFigure = presentFleets.length ? buildFleetFigure(presentFleets, darkMode) : null;

  const renderHeatmapSection = (title, units, heatmap, emptyMessage, caveat) => (
    <div className="generation-section">
      <h2 className="generation-section-title">{title}</h2>
      {caveat && <div className="generation-caveat">{caveat}</div>}
      {units.length === 0 && <div className="generation-empty">{emptyMessage}</div>}
      {heatmap && (
        <div className="chart-container">
          <Plot
            data={[
              {
                x: heatmap.x,
                y: heatmap.y,
                z: heatmap.z,
                customdata: heatmap.customdata,
                type: 'heatmap',
                colorscale: 'YlGnBu',
                zmin: 0,
                zmax: 1,
                hovertemplate:
                  '%{y}<br>%{x|%a %d %b %H:%M}<br>Utilisation: %{z:.0%}' +
                  '<br>%{customdata[0]:.0f} MW · %{customdata[1]}<extra></extra>',
                colorbar: { title: 'Utilisation' },
              },
            ]}
            layout={{
              plot_bgcolor: darkMode ? '#1a1a1a' : 'white',
              paper_bgcolor: darkMode ? '#1a1a1a' : 'white',
              font: { color: darkMode ? '#f5f5f5' : '#333' },
              margin: { l: 260, r: 30, t: 20, b: 60 },
              height: Math.max(220, units.length * 32 + 100),
              xaxis: { tickformat: '%a %H:%M', gridcolor: darkMode ? '#404040' : '#e0e0e0' },
              yaxis: { automargin: true },
            }}
            useResizeHandler
            style={{ width: '100%' }}
            config={{ displayModeBar: true, displaylogo: false }}
          />
        </div>
      )}
    </div>
  );

  return (
    <div className={`generation-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="generation-controls">
        <label htmlFor="generation-region">Region</label>
        <select id="generation-region" value={region} onChange={(e) => setRegion(e.target.value)}>
          {REGIONS.map((r) => (
            <option key={r} value={r}>
              {r}
            </option>
          ))}
        </select>
        {data.run_datetime && (
          <span className="generation-meta">Pre-dispatch run: {new Date(data.run_datetime).toLocaleString()}</span>
        )}
      </div>

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <p>Loading generation forecast…</p>
        </div>
      )}

      {!loading && error && <div className="generation-error">{error}</div>}

      {!loading && !error && (
        <>
          {renderHeatmapSection('Coal', coalUnits, coalHeatmap, 'No coal units inferable for this region right now.')}
          {renderHeatmapSection('Gas', gasUnits, gasHeatmap, 'No gas units inferable for this region right now.')}
          {renderHeatmapSection(
            'Battery',
            batteryUnits,
            batteryHeatmap,
            'No battery units inferable for this region right now.',
            'Discharge-only inference; charging intervals unreliable/absent.'
          )}

          <div className="generation-section">
            <h2 className="generation-section-title">Wind + Solar (fleet)</h2>
            {!windFleet && !solarFleet && (
              <div className="generation-empty">No wind or solar fleet data for this region right now.</div>
            )}
            {windFleet && <div className="generation-meta">Wind: {coverageSubtitle(windFleet)}</div>}
            {solarFleet && <div className="generation-meta">Solar: {coverageSubtitle(solarFleet)}</div>}
            {fleetFigure && (
              <div className="chart-container">
                <Plot
                  data={fleetFigure.data}
                  layout={fleetFigure.layout}
                  useResizeHandler
                  style={{ width: '100%' }}
                  config={{ displayModeBar: true, displaylogo: false }}
                />
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}

export default GenerationForecastPage;
