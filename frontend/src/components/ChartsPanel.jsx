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
import { formatDateTimeShort, getTodayInChileDateInput } from '../utils/dateTime';
import {
  getVariableDisplayName,
  getUnitForKey,
} from '../utils/weatherVariables';

const FIXED_VARIABLE_KEYS = ['Temp', 'Hum', 'Precip', 'Rad', 'Vel', 'Dir'];

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
);

function isNumericValue(value) {
  return value !== null && value !== '' && !Number.isNaN(Number(value));
}

function scaleValue(value) {
  return Number((value / 10).toFixed(1));
}

function toSeries(rows, key) {
  return rows
    .slice()
    .reverse()
    .map((row) => (isNumericValue(row[key]) ? scaleValue(Number(row[key])) : null));
}

export default function ChartsPanel({ refreshTick = 0 }) {
  const [filters, setFilters] = useState({
    desde: '',
    hasta: getTodayInChileDateInput(),
    limit: 200,
  });
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [selectedKey, setSelectedKey] = useState('Temp');

  const labels = useMemo(
    () => rows.slice().reverse().map((row) => formatDateTimeShort(row.received_at || row.Timestamp || '')),
    [rows],
  );

  async function loadChart() {
    setLoading(true);
    setError('');
    try {
      const data = await fetchWeatherRange(filters);
      setRows(data);
    } catch {
      setError('No se pudieron cargar datos para el grafico.');
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadChart();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshTick]);

  const chartData = useMemo(() => {
    const selectedKeys = selectedKey ? [selectedKey] : [];
    const palette = ['#ff9800', '#2196f3', '#00e5a0', '#f06292', '#ffd54f', '#7e57c2'];
    const datasets = selectedKeys.map((key, index) => ({
      label: getUnitForKey(key)
        ? `${getVariableDisplayName(key)} (${getUnitForKey(key)})`
        : getVariableDisplayName(key),
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
    const selectedLabel = getVariableDisplayName(selectedKey);
    const yTitle = selectedKey ? (unit ? `${selectedLabel} (${unit})` : selectedLabel) : 'Valor';

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
            text: 'Fecha y hora (Chile)',
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
            callback: (value) => {
              const display = typeof value === 'number' ? value.toFixed(1) : String(value);
              return unit ? `${display} ${unit}` : `${display}`;
            },
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
              const display = value === null || value === undefined ? '' : Number(value).toFixed(1);
              return unit ? `${label}: ${display} ${unit}` : `${label}: ${display}`;
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
          <select value={selectedKey} onChange={onVariableChange}>
            <option value="">Seleccionar variable</option>
            {FIXED_VARIABLE_KEYS.map((key) => (
              <option key={`var-${key}`} value={key}>
                {getUnitForKey(key)
                  ? `${getVariableDisplayName(key)} (${getUnitForKey(key)})`
                  : getVariableDisplayName(key)}
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
