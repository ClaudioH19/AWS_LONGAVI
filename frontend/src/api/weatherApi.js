const API_BASE = '/';

function buildRangeQuery({ desde, hasta, device, limit, offset }) {
  const params = new URLSearchParams();
  if (limit) params.set('limit', String(limit));
  if (offset) params.set('offset', String(offset));
  if (desde) params.set('desde', desde);
  if (hasta) params.set('hasta', hasta);
  if (device) params.set('device', device);
  return params.toString();
}

export async function fetchWeatherPage(filters = {}) {
  const query = buildRangeQuery(filters);
  const response = await fetch(`${API_BASE}weather/range?${query}`, {
    cache: 'no-store',
  });
  if (!response.ok) {
    throw new Error('No se pudo obtener weather/range');
  }
  const json = await response.json();
  return {
    data: Array.isArray(json.data) ? json.data : [],
    hasMore: Boolean(json.has_more),
    offset: Number(json.offset) || 0,
  };
}

export async function fetchWeatherRange(filters = {}) {
  const page = await fetchWeatherPage(filters);
  return page.data;
}

export async function fetchAllWeatherRange(filters = {}, { pageSize = 5000, maxRows = 50000 } = {}) {
  const rows = [];
  let offset = 0;

  while (rows.length < maxRows) {
    const limit = Math.min(pageSize, maxRows - rows.length);
    const page = await fetchWeatherPage({ ...filters, limit, offset });
    rows.push(...page.data);
    if (!page.hasMore || page.data.length === 0) break;
    offset += page.data.length;
  }

  return rows;
}

export async function fetchHealth() {
  const response = await fetch(`${API_BASE}health`, {
    cache: 'no-store',
  });
  if (!response.ok) {
    throw new Error('No se pudo obtener health');
  }
  return response.json();
}

export async function fetchLatest() {
  const response = await fetch(`${API_BASE}weather/latest`, {
    cache: 'no-store',
  });
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error('No se pudo obtener weather/latest');
  }
  return response.json();
}

export function buildExportUrl(format, filters = {}) {
  const query = buildRangeQuery(filters);
  return `${API_BASE}weather/export/${format}?${query}`;
}
