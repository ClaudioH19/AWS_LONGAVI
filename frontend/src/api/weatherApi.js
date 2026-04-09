const API_BASE = '/';

const FIELD_ALIASES = {
  temperature: 'temperature',
  Temperature: 'temperature',
  temp: 'temperature',
  Temp: 'temperature',
  humidity: 'Humidity',
  Humidity: 'Humidity',
  hum: 'Humidity',
  Hum: 'Humidity',
};

function buildRangeQuery({ desde, hasta, limit }) {
  const params = new URLSearchParams();
  if (limit) params.set('limit', String(limit));
  if (desde) params.set('desde', desde);
  if (hasta) params.set('hasta', hasta);
  return params.toString();
}

function normalizeWeatherRow(row = {}) {
  // El servidor ya devuelve las claves mapeadas según payload.js.
  // No crear claves adicionales (como ch0/CH0) aquí para evitar columnas "variables".
  return { ...row };
}

function normalizeRows(rows = []) {
  return rows.map((row) => normalizeWeatherRow(row));
}

async function fetchWeatherLatest() {
  const response = await fetch(`${API_BASE}weather/latest`);
  if (!response.ok) {
    throw new Error('No se pudo obtener weather/latest');
  }
  const json = await response.json();
  return normalizeWeatherRow(json);
}

export async function fetchWeatherRange(filters = {}) {
  const query = buildRangeQuery(filters);
  const response = await fetch(`${API_BASE}weather/range?${query}`);
  if (response.ok) {
    const json = await response.json();
    const rows = normalizeRows(json.data || []);
    if (rows.length > 0) {
      return rows;
    }
  }

  // Fallback real: usa el ultimo dato real si no hay rango.
  try {
    const latest = await fetchWeatherLatest();
    return latest ? [latest] : [];
  } catch {
    // Si latest tampoco existe, se propaga error para mostrar estado real.
  }

  throw new Error('No hay datos reales disponibles en weather/range ni weather/latest');
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
