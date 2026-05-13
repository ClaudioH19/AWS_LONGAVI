import { useEffect, useMemo, useState } from 'react';
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Title,
  Tooltip,
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';
import { fetchWeatherRange } from '../api/weatherApi';
import { formatDateTimeShort, getTodayInChileDateInput } from '../utils/dateTime';
import {
  getVariableDisplayName,
  getUnitForKey,
} from '../utils/weatherVariables';

const FIXED_VARIABLE_KEYS = ['Temp', 'Hum', 'Precip', 'Rad', 'Vel', 'Dir'];
const WEEKDAY_LABELS = ['Lun', 'Mar', 'Mie', 'Jue', 'Vie', 'Sab', 'Dom'];

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
);

function isNumericValue(value) {
  return value !== null && value !== '' && !Number.isNaN(Number(value));
}

function scaleValue(value) {
  return Number((value / 10).toFixed(2));
}

function parseDateParts(value) {
  if (!value) return null;
  const text = String(value).trim();
  const match = text.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  return { year, month, day };
}

function toDateInputFromUTC(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function getIsoWeekKeyFromDateParts(year, month, day) {
  const date = new Date(Date.UTC(year, month - 1, day));
  const dayOfWeek = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - dayOfWeek);
  const isoYear = date.getUTCFullYear();
  const yearStart = new Date(Date.UTC(isoYear, 0, 1));
  const weekNo = Math.ceil((((date - yearStart) / 86400000) + 1) / 7);
  return `${isoYear}-W${String(weekNo).padStart(2, '0')}`;
}

function getCurrentWeekInput() {
  const today = getTodayInChileDateInput();
  const parts = parseDateParts(today);
  if (!parts) return '';
  return getIsoWeekKeyFromDateParts(parts.year, parts.month, parts.day);
}

function getWeekBounds(weekInput) {
  const match = String(weekInput || '').match(/^(\d{4})-W(\d{2})$/);
  if (!match) return { desde: '', hasta: '' };

  const isoYear = Number(match[1]);
  const isoWeek = Number(match[2]);
  if (!Number.isFinite(isoYear) || !Number.isFinite(isoWeek) || isoWeek < 1 || isoWeek > 53) {
    return { desde: '', hasta: '' };
  }

  const jan4 = new Date(Date.UTC(isoYear, 0, 4));
  const jan4Day = jan4.getUTCDay() || 7;
  const mondayWeek1 = new Date(jan4);
  mondayWeek1.setUTCDate(jan4.getUTCDate() - jan4Day + 1);

  const monday = new Date(mondayWeek1);
  monday.setUTCDate(mondayWeek1.getUTCDate() + ((isoWeek - 1) * 7));
  const sunday = new Date(monday);
  sunday.setUTCDate(monday.getUTCDate() + 6);

  return {
    desde: toDateInputFromUTC(monday),
    hasta: toDateInputFromUTC(sunday),
  };
}

function getTimestampForRow(row) {
  return row.Timestamp || row.received_at || '';
}

function getWeekdayIndex(value) {
  const parts = parseDateParts(value);
  if (!parts) return null;
  const jsDay = new Date(Date.UTC(parts.year, parts.month - 1, parts.day)).getUTCDay();
  return (jsDay + 6) % 7;
}

function toSeries(rows, key) {
  return rows
    .slice()
    .reverse()
    .map((row) => (isNumericValue(row[key]) ? scaleValue(Number(row[key])) : null));
}

function toWeeklyAverageSeries(rows, key, selectedWeek) {
  const totals = Array(7).fill(0);
  const counts = Array(7).fill(0);

  rows.forEach((row) => {
    const timestamp = getTimestampForRow(row);
    const dateParts = parseDateParts(timestamp);
    if (!dateParts) return;

    const rowWeek = getIsoWeekKeyFromDateParts(dateParts.year, dateParts.month, dateParts.day);
    if (rowWeek !== selectedWeek) return;
    if (!isNumericValue(row[key])) return;

    const weekdayIndex = getWeekdayIndex(timestamp);
    if (weekdayIndex === null) return;

    totals[weekdayIndex] += Number(row[key]) / 10;
    counts[weekdayIndex] += 1;
  });

  return totals.map((sum, index) => (counts[index] ? Number((sum / counts[index]).toFixed(2)) : null));
}

export default function ChartsPanel({ refreshTick = 0 }) {
  const [filters, setFilters] = useState({
    week: getCurrentWeekInput(),
    limit: 200,
  });
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [selectedKey, setSelectedKey] = useState('Temp');
  const [chartType, setChartType] = useState('line');

  const lineLabels = useMemo(
    () => rows.slice().reverse().map((row) => formatDateTimeShort(getTimestampForRow(row))),
    [rows],
  );

  async function loadChart() {
    const { desde, hasta } = getWeekBounds(filters.week);
    setLoading(true);
    setError('');
    try {
      const data = await fetchWeatherRange({
        desde,
        hasta,
        limit: filters.limit,
      });
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
    if (!selectedKey) return { labels: [], datasets: [] };

    const color = '#ff9800';
    const baseLabel = getUnitForKey(selectedKey)
      ? `${getVariableDisplayName(selectedKey)} (${getUnitForKey(selectedKey)})`
      : getVariableDisplayName(selectedKey);

    if (chartType === 'bar') {
      return {
        labels: WEEKDAY_LABELS,
        datasets: [
          {
            label: `Promedio semanal ${baseLabel}`,
            data: toWeeklyAverageSeries(rows, selectedKey, filters.week),
            borderColor: color,
            backgroundColor: `${color}77`,
            borderRadius: 6,
            borderWidth: 1.5,
          },
        ],
      };
    }

    return {
      labels: lineLabels,
      datasets: [
        {
          label: baseLabel,
          data: toSeries(rows, selectedKey),
          borderColor: color,
          backgroundColor: `${color}33`,
          spanGaps: true,
          tension: 0.25,
          pointRadius: 1.5,
          borderWidth: 2,
        },
      ],
    };
  }, [chartType, filters.week, lineLabels, rows, selectedKey]);

  const chartOptions = useMemo(() => {
    const unit = getUnitForKey(selectedKey);
    const selectedLabel = getVariableDisplayName(selectedKey);
    const yTitle = selectedKey ? (unit ? `${selectedLabel} (${unit})` : selectedLabel) : 'Valor';
    const isBarChart = chartType === 'bar';

    return {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      elements: {
        point: {
          radius: isBarChart ? 2 : 0,
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
            autoSkip: !isBarChart,
            maxTicksLimit: isBarChart ? undefined : 10,
            color: '#4b5563',
            font: {
              size: 11,
              weight: '600',
            },
          },
          title: {
            display: true,
            text: isBarChart ? 'Dia de semana' : 'Fecha y hora (Chile)',
            color: '#374151',
          },
        },
        y: {
          grace: '8%',
          beginAtZero: false,
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
              const display = typeof value === 'number' ? value.toFixed(2) : String(value);
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
              const display = value === null || value === undefined ? '' : Number(value).toFixed(2);
              return unit ? `${label}: ${display} ${unit}` : `${label}: ${display}`;
            },
          },
        },
      },
    };
  }, [chartType, selectedKey]);

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
          Semana
          <input type="week" name="week" value={filters.week} onChange={onFilterChange} />
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
        <div className="chart-type-toggle" role="group" aria-label="Tipo de grafico">
          <button
            type="button"
            className={chartType === 'line' ? 'is-active' : ''}
            onClick={() => setChartType('line')}
          >
            Linea
          </button>
          <button
            type="button"
            className={chartType === 'bar' ? 'is-active' : ''}
            onClick={() => setChartType('bar')}
          >
            Barras
          </button>
        </div>
        <button type="button" onClick={loadChart} disabled={loading}>
          {loading ? 'Cargando...' : 'Actualizar grafico'}
        </button>
      </div>

      <div className="chart-wrap">
        {error && <p className="error">{error}</p>}
        {loading && !error && <p className="muted">Actualizando grafico...</p>}
        {!error && selectedKey && (
          <div className="chart-canvas-shell">
            {chartType === 'bar'
              ? <Bar data={chartData} options={chartOptions} />
              : <Line data={chartData} options={chartOptions} />}
          </div>
        )}
        {!error && !selectedKey && !loading && (
          <p className="muted">Selecciona al menos una variable para graficar.</p>
        )}
      </div>
    </section>
  );
}
