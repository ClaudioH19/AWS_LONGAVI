const API_BASE = '/';

function buildRangeQuery({ desde, hasta, limit }) {
  const params = new URLSearchParams();
  if (limit) params.set('limit', String(limit));
  if (desde) params.set('desde', desde);
  if (hasta) params.set('hasta', hasta);
  return params.toString();
}

export async function fetchWeatherRange(filters = {}) {
  const query = buildRangeQuery(filters);
  const response = await fetch(`${API_BASE}weather/range?${query}`);
  if (!response.ok) {
    throw new Error('No se pudo obtener weather/range');
  }
  const json = await response.json();
  return Array.isArray(json.data) ? json.data : [];
}

export async function fetchHealth() {
  const response = await fetch(`${API_BASE}health`);
  if (!response.ok) {
    throw new Error('No se pudo obtener health');
  }
  return response.json();
}

export function buildExportUrl(format, filters = {}) {
  const query = buildRangeQuery(filters);
  return `${API_BASE}weather/export/${format}?${query}`;
}
