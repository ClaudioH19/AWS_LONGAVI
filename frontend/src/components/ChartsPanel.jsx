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
  WEATHER_FIXED_KEYS,
  getVariableDisplayName,
  getUnitForKey,
  scaleWeatherValue,
} from '../utils/weatherVariables';

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
    .map((row) => scaleWeatherValue(key, row[key]));
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

    const scaledValue = scaleWeatherValue(key, row[key]);
    if (scaledValue === null) return;

    const weekdayIndex = getWeekdayIndex(timestamp);
    if (weekdayIndex === null) return;

    totals[weekdayIndex] += scaledValue;
    counts[weekdayIndex] += 1;
  });

  return totals.map((sum, index) => (counts[index] ? Number((sum / counts[index]).toFixed(2)) : null));
}

function buildChartOptions({ selectedKey, isBarChart }) {
  const unit = getUnitForKey(selectedKey);
  const selectedLabel = getVariableDisplayName(selectedKey);
  const yTitle = unit ? `${selectedLabel} (${unit})` : selectedLabel;

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
    scales: {
      x: {
        grid: {
          color: 'rgba(148, 163, 184, 0.14)',
        },
        ticks: {
          color: '#4c5a55',
          maxRotation: 0,
          autoSkip: !isBarChart,
          maxTicksLimit: isBarChart ? undefined : 10,
        },
        title: {
          display: true,
          text: isBarChart ? 'Dia de semana' : 'Fecha y hora',
          color: '#33403c',
        },
      },
      y: {
        grace: '8%',
        grid: {
          color: 'rgba(148, 163, 184, 0.16)',
        },
        ticks: {
          color: '#4c5a55',
          callback: (value) => (unit ? `${value} ${unit}` : `${value}`),
        },
        title: {
          display: true,
          text: yTitle,
          color: '#33403c',
        },
      },
    },
    plugins: {
      legend: {
        labels: {
          color: '#33403c',
          font: {
            weight: '700',
          },
        },
      },
      tooltip: {
        backgroundColor: 'rgba(16, 24, 22, 0.94)',
        titleColor: '#eef6f2',
        bodyColor: '#eef6f2',
        callbacks: {
          label: (context) => {
            const label = context.dataset?.label || selectedLabel;
            const value = context.parsed?.y;
            if (value === null || value === undefined) {
              return `${label}: sin dato`;
            }
            return unit ? `${label}: ${value.toFixed(2)} ${unit}` : `${label}: ${value.toFixed(2)}`;
          },
        },
      },
    },
  };
}

function ChartCard({ title, subtitle, children }) {
  return (
    <article className="chart-panel">
      <div className="chart-header">
        <h3>{title}</h3>
        <p>{subtitle}</p>
      </div>
      <div className="chart-canvas-shell">{children}</div>
    </article>
  );
}

export default function ChartsPanel({ refreshTick = 0 }) {
  const [filters, setFilters] = useState({
    desde: '',
    hasta: getTodayInChileDateInput(),
    week: getCurrentWeekInput(),
    limit: 240,
  });
  const [lineRows, setLineRows] = useState([]);
  const [barRows, setBarRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [lineKey, setLineKey] = useState('Temp');
  const [barKey, setBarKey] = useState('Hum');

  const lineLabels = useMemo(
    () => lineRows.slice().reverse().map((row) => formatDateTimeShort(getTimestampForRow(row))),
    [lineRows],
  );

  async function loadCharts() {
    const { desde: weekDesde, hasta: weekHasta } = getWeekBounds(filters.week);

    setLoading(true);
    setError('');
    try {
      const [lineData, barData] = await Promise.all([
        fetchWeatherRange({
          desde: filters.desde,
          hasta: filters.hasta,
          limit: filters.limit,
        }),
        fetchWeatherRange({
          desde: weekDesde,
          hasta: weekHasta,
          limit: filters.limit,
        }),
      ]);

      setLineRows(lineData);
      setBarRows(barData);
    } catch {
      setError('No se pudieron cargar los graficos.');
      setLineRows([]);
      setBarRows([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadCharts();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshTick]);

  const lineData = useMemo(() => {
    const unit = getUnitForKey(lineKey);
    const label = getVariableDisplayName(lineKey);

    return {
      labels: lineLabels,
      datasets: [
        {
          label: unit ? `${label} (${unit})` : label,
          data: toSeries(lineRows, lineKey),
          borderColor: '#0f766e',
          backgroundColor: 'rgba(15, 118, 110, 0.18)',
          tension: 0.28,
          borderWidth: 2.5,
          fill: true,
          spanGaps: true,
        },
      ],
    };
  }, [lineKey, lineLabels, lineRows]);

  const barData = useMemo(() => {
    const unit = getUnitForKey(barKey);
    const label = getVariableDisplayName(barKey);

    return {
      labels: WEEKDAY_LABELS,
      datasets: [
        {
          label: unit ? `Promedio semanal ${label} (${unit})` : `Promedio semanal ${label}`,
          data: toWeeklyAverageSeries(barRows, barKey, filters.week),
          borderColor: '#d97706',
          backgroundColor: 'rgba(217, 119, 6, 0.35)',
          borderRadius: 10,
          borderWidth: 1.5,
        },
      ],
    };
  }, [barKey, barRows, filters.week]);

  const lineOptions = useMemo(
    () => buildChartOptions({ selectedKey: lineKey, isBarChart: false }),
    [lineKey],
  );

  const barOptions = useMemo(
    () => buildChartOptions({ selectedKey: barKey, isBarChart: true }),
    [barKey],
  );

  function onFilterChange(event) {
    const { name, value } = event.target;
    setFilters((prev) => ({ ...prev, [name]: value }));
  }

  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <span className="panel-kicker">Analitica</span>
          <h2>Tendencias y promedios operativos</h2>
        </div>
        <button type="button" onClick={loadCharts} disabled={loading}>
          {loading ? 'Actualizando...' : 'Actualizar panel'}
        </button>
      </div>

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
          Semana barra
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
          Variable linea
          <select value={lineKey} onChange={(event) => setLineKey(event.target.value)}>
            {WEATHER_FIXED_KEYS.map((key) => (
              <option key={`line-${key}`} value={key}>
                {getVariableDisplayName(key)}
              </option>
            ))}
          </select>
        </label>
        <label className="variable-inline-selector">
          Variable barras
          <select value={barKey} onChange={(event) => setBarKey(event.target.value)}>
            {WEATHER_FIXED_KEYS.map((key) => (
              <option key={`bar-${key}`} value={key}>
                {getVariableDisplayName(key)}
              </option>
            ))}
          </select>
        </label>
      </div>

      {error && <p className="error">{error}</p>}
      {loading && !error && <p className="muted">Consultando historico persistido...</p>}

      <div className="chart-grid">
        <ChartCard
          title="Grafico de linea"
          subtitle="Evolucion temporal de la variable seleccionada."
        >
          <Line data={lineData} options={lineOptions} />
        </ChartCard>

        <ChartCard
          title="Grafico de barras"
          subtitle="Promedio diario de la semana elegida."
        >
          <Bar data={barData} options={barOptions} />
        </ChartCard>
      </div>
    </section>
  );
}
