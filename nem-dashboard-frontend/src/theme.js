/**
 * Shared chart + data colors. Single source of truth — component-local
 * REGION_COLORS/FUEL_COLORS maps should import from here instead.
 * Hue-per-region mapping is legacy (NSW blue, VIC orange, ...); values are
 * tuned to pass CVD/contrast checks on both light and dark surfaces.
 */

const BASE_REGION_COLORS = {
  NSW: '#3b82d9',
  VIC: '#d97733',
  QLD: '#2fa36b',
  SA: '#e25563',
  TAS: '#8b6fe8',
  WA: '#2e9ba8',
  NT: '#c95fa4',
};

// Both 'NSW' and 'NSW1' style keys are used across pages.
export const REGION_COLORS = Object.fromEntries(
  Object.entries(BASE_REGION_COLORS).flatMap(([k, v]) => [
    [k, v],
    [`${k}1`, v],
  ])
);

export const FUEL_COLORS = {
  Coal: '#6b7280',
  Gas: '#e08a3c',
  Hydro: '#4fb3d9',
  Wind: '#2fa36b',
  Solar: '#e7c13d',
  Battery: '#8b6fe8',
  Diesel: '#a0715c',
  Biomass: '#7fa65a',
  Unknown: '#6e7a90',
};

/** Chart chrome colors for the current mode (Plotly can't read CSS vars). */
export function chartColors(darkMode) {
  return darkMode
    ? {
        text: '#e8edf7',
        text2: '#97a3ba',
        grid: '#212b3d',
        axisLine: '#2e3b54',
        hoverBg: '#1a2233',
        hoverBorder: '#2e3b54',
        accent: '#8b7cff',
        ok: '#34c98e',
        warn: '#e8a33d',
        danger: '#e5606b',
      }
    : {
        text: '#17202f',
        text2: '#57647c',
        grid: '#e9edf4',
        axisLine: '#c4cedf',
        hoverBg: '#ffffff',
        hoverBorder: '#c4cedf',
        accent: '#5f4be8',
        ok: '#118a5e',
        warn: '#a66b0f',
        danger: '#c93b49',
      };
}

/**
 * Base Plotly layout: transparent background so the CSS card behind the
 * chart shows through, recessive grid, IBM Plex type. Spread first, then
 * override per chart: { ...baseLayout(darkMode), title: ... }.
 */
export function baseLayout(darkMode) {
  const c = chartColors(darkMode);
  return {
    plot_bgcolor: 'transparent',
    paper_bgcolor: 'transparent',
    font: { family: "'IBM Plex Sans', sans-serif", size: 12, color: c.text2 },
    xaxis: { gridcolor: c.grid, linecolor: c.axisLine, zerolinecolor: c.grid },
    yaxis: { gridcolor: c.grid, linecolor: c.axisLine, zerolinecolor: c.grid },
    hoverlabel: {
      bgcolor: c.hoverBg,
      bordercolor: c.hoverBorder,
      font: { family: "'IBM Plex Mono', monospace", size: 12, color: c.text },
    },
    legend: { font: { color: c.text2 } },
    margin: { t: 36, r: 16, b: 40, l: 56 },
  };
}
