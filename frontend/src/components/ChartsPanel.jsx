import { useEffect, useMemo, useState } from 'react';
import {
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Title,
  Tooltip,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { fetchWeatherRange } from '../api/weatherApi';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
);

function getToday() {
  return new Date().toISOString().split('T')[0];
}

function isNumericValue(value) {
  return value !== null && value !== '' && !Number.isNaN(Number(value));
}

function scaleValue(value) {
  return Number((value / 10).toFixed(2));
}

function getUnitForKey(key = '') {
  const normalized = key.toLowerCase();

  if (/temp|temperature|temperatura/.test(normalized)) return '°C';
  if (/humidity|humedad|rh|hr/.test(normalized)) return '%';
  if (/solar|radiaci|irradiance|radiation/.test(normalized)) return 'W/m2';
  if (/press|presion|pressure/.test(normalized)) return 'hPa';
  if (/wind|viento/.test(normalized)) return 'm/s';
  if (/rain|lluvia|precip/.test(normalized)) return 'mm';

  return '';
}

function formatDateTimeLabel(rawValue) {
  if (!rawValue) return '';

  const source = String(rawValue).trim();
  const normalized = source.includes('T') ? source : source.replace(' ', 'T');
  const date = new Date(normalized);

  if (Number.isNaN(date.getTime())) {
    return source.replace('T', ' ');
  }

  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(date.getDate())}/${pad(date.getMonth() + 1)} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function toSeries(rows, key) {
  return rows
    .slice()
    .reverse()
    .map((row) => (isNumericValue(row[key]) ? scaleValue(Number(row[key])) : null));
}

export default function ChartsPanel() {
  const [filters, setFilters] = useState({
    desde: '',
    hasta: getToday(),
    limit: 200,
  });
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [selectedKey, setSelectedKey] = useState('');

  const labels = useMemo(
    () => rows.slice().reverse().map((row) => formatDateTimeLabel(row.received_at || row.Timestamp || '')),
    [rows],
  );

  const numericKeys = useMemo(() => {
    if (!rows.length) return [];
    const excluded = new Set([
      'id',
      'received_at',
      'raw_json',
      'Timestamp',
      'DeviceID',
      'DeviceType',
      'DeviceVersion',
      'deviceid',
      'devicetype',
      'deviceversion',
    ]);
    const map = new Map();

    rows.forEach((row) => {
      Object.keys(row).forEach((key) => {
        if (excluded.has(key)) return;
        if (!map.has(key)) map.set(key, 0);
        if (isNumericValue(row[key])) {
          map.set(key, map.get(key) + 1);
        }
      });
    });

    return Array.from(map.entries())
      .filter(([, score]) => score > 0)
      .sort((a, b) => b[1] - a[1])
      .map(([key]) => key);
  }, [rows]);

  useEffect(() => {
    if (!numericKeys.length) {
      setSelectedKey('');
      return;
    }

    const preferred = [
      'temperature',
      'Temperature',
      'Humidity',
      'humidity',
      'ch1',
      'CH1',
      'ch2',
      'CH2',
      'ch0',
      'CH0',
      'temperatura',
      'humedad',
    ];
    const defaults = preferred.filter((key) => numericKeys.includes(key));
    const fallback = numericKeys.slice(0, 1);
    const next = (defaults.length ? defaults : fallback).slice(0, 1);

    setSelectedKey((current) => {
      if (current && numericKeys.includes(current)) return current;
      return next[0] || '';
    });
  }, [numericKeys]);

  async function loadChart() {
    setLoading(true);
    setError('');
    try {
      const data = await fetchWeatherRange(filters);
      setRows(data);
    } catch (e) {
      setError('No se pudieron cargar datos para el gráfico.');
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadChart();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const chartData = useMemo(() => {
    const selectedKeys = selectedKey ? [selectedKey] : [];
    const palette = ['#ff9800', '#2196f3', '#00e5a0', '#f06292', '#ffd54f', '#7e57c2'];
    const datasets = selectedKeys.map((key, index) => ({
      label: getUnitForKey(key) ? `${key} (${getUnitForKey(key)})` : key,
      data: toSeries(rows, key),
      borderColor: palette[index % palette.length],
      backgroundColor: `${palette[index % palette.length]}33`,
      spanGaps: true,
      tension: 0.25,
      pointRadius: 1.5,
      borderWidth: 2,
    }));

    return { labels, datasets };
  }, [labels, rows, selectedKey]);

  const chartOptions = useMemo(() => {
    const unit = getUnitForKey(selectedKey);
    const yTitle = selectedKey ? (unit ? `${selectedKey} (${unit})` : selectedKey) : 'Valor';

    return {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      elements: {
        point: {
          radius: 0,
          hoverRadius: 4,
        },
      },
      layout: {
        padding: {
          top: 8,
          right: 12,
          bottom: 4,
          left: 4,
        },
      },
      scales: {
        x: {
          grid: {
            display: true,
            drawOnChartArea: true,
            color: 'rgba(200, 208, 220, 0.12)',
            tickColor: 'rgba(200, 208, 220, 0.22)',
            borderDash: [3, 3],
            lineWidth: 1,
          },
          border: {
            color: 'rgba(200, 208, 220, 0.35)',
          },
          ticks: {
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 10,
            color: '#4b5563',
            font: {
              size: 11,
              weight: '600',
            },
          },
          title: {
            display: true,
            text: 'Fecha y hora',
            color: '#374151',
          },
        },
        y: {
          grace: '8%',
          grid: {
            display: true,
            drawOnChartArea: true,
            color: 'rgba(200, 208, 220, 0.16)',
            tickColor: 'rgba(200, 208, 220, 0.24)',
            borderDash: [4, 4],
            lineWidth: 1,
          },
          border: {
            color: 'rgba(200, 208, 220, 0.35)',
          },
          title: {
            display: true,
            text: yTitle,
            color: '#374151',
          },
          ticks: {
            color: '#4b5563',
            font: {
              size: 11,
              weight: '600',
            },
            callback: (value) => (unit ? `${value} ${unit}` : `${value}`),
          },
        },
      },
      plugins: {
        legend: {
          labels: {
            color: '#374151',
            boxWidth: 18,
            boxHeight: 2,
            usePointStyle: false,
            font: {
              size: 12,
              weight: '700',
            },
          },
        },
        tooltip: {
          backgroundColor: 'rgba(12, 16, 22, 0.95)',
          borderColor: 'rgba(200, 208, 220, 0.25)',
          borderWidth: 1,
          titleColor: 'rgba(238, 242, 248, 0.95)',
          bodyColor: 'rgba(238, 242, 248, 0.95)',
          callbacks: {
            label: (context) => {
              const label = context.dataset?.label || selectedKey || 'Valor';
              const value = context.parsed?.y;
              return unit ? `${label}: ${value} ${unit}` : `${label}: ${value}`;
            },
          },
        },
      },
    };
  }, [selectedKey]);

  const selectedKeys = useMemo(() => (selectedKey ? [selectedKey] : []), [selectedKey]);

  function onFilterChange(event) {
    const { name, value } = event.target;
    setFilters((prev) => ({ ...prev, [name]: value }));
  }

  function onVariableChange(event) {
    setSelectedKey(event.target.value);
  }

  return (
    <section className="panel">
      <h2>Graficos por variable en el tiempo</h2>
      <div className="panel-toolbar">
        <label>
          Desde
          <input type="date" name="desde" value={filters.desde} onChange={onFilterChange} />
        </label>
        <label>
          Hasta
          <input type="date" name="hasta" value={filters.hasta} onChange={onFilterChange} />
        </label>
        <label>
          Limite
          <input
            type="number"
            min="1"
            max="5000"
            name="limit"
            value={filters.limit}
            onChange={onFilterChange}
          />
        </label>
        <label className="variable-inline-selector">
          Variable
          <select value={selectedKey} onChange={onVariableChange} disabled={numericKeys.length === 0}>
            <option value="">Seleccionar variable</option>
            {numericKeys.map((key) => (
              <option key={`var-${key}`} value={key}>
                {key}
              </option>
            ))}
          </select>
        </label>
        <button type="button" onClick={loadChart} disabled={loading}>
          {loading ? 'Cargando...' : 'Actualizar grafico'}
        </button>
      </div>

      <div className="chart-wrap">
        {error && <p className="error">{error}</p>}
        {loading && !error && <p className="muted">Actualizando grafico...</p>}
        {!error && selectedKeys.length > 0 && (
          <div className="chart-canvas-shell">
            <Line data={chartData} options={chartOptions} />
          </div>
        )}
        {!error && selectedKeys.length === 0 && !loading && (
          <p className="muted">Selecciona al menos una variable para graficar.</p>
        )}
      </div>
    </section>
  );
}
