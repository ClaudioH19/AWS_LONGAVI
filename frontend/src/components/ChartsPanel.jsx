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
import { Chart } from 'react-chartjs-2';
import { fetchWeatherRange } from '../api/weatherApi';
import { getTodayInChileDateInput } from '../utils/dateTime';
import {
  WEATHER_FIXED_KEYS,
  getVariableDisplayName,
  getUnitForKey,
  scaleWeatherValue,
} from '../utils/weatherVariables';

const WEEKDAY_LABELS = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom'];

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
  const match = String(value || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;

  return {
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
  };
}

function toDateInputFromUTC(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function getCurrentWeekBounds() {
  const today = parseDateParts(getTodayInChileDateInput());
  if (!today) {
    return { desde: '', hasta: '' };
  }

  const todayDate = new Date(Date.UTC(today.year, today.month - 1, today.day));
  const jsDay = todayDate.getUTCDay();
  const mondayOffset = jsDay === 0 ? -6 : 1 - jsDay;

  const monday = new Date(todayDate);
  monday.setUTCDate(todayDate.getUTCDate() + mondayOffset);

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
  const text = String(value || '').trim();
  const match = text.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!match) return null;

  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  const jsDay = date.getUTCDay();
  return (jsDay + 6) % 7;
}

function buildWeeklySeries(rows, key, { fillMissingWithZero = false } = {}) {
  const totals = Array(7).fill(0);
  const counts = Array(7).fill(0);

  rows.forEach((row) => {
    const weekdayIndex = getWeekdayIndex(getTimestampForRow(row));
    const scaledValue = scaleWeatherValue(key, row[key]);

    if (weekdayIndex === null || scaledValue === null) {
      return;
    }

    totals[weekdayIndex] += scaledValue;
    counts[weekdayIndex] += 1;
  });

  return totals.map((sum, index) => {
    if (!counts[index]) {
      return fillMissingWithZero ? 0 : null;
    }
    return Number((sum / counts[index]).toFixed(2));
  });
}

function buildChartOptions({ lineKey, barKey }) {
  const lineUnit = getUnitForKey(lineKey);
  const barUnit = getUnitForKey(barKey);

  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      mode: 'index',
      intersect: false,
    },
    scales: {
      x: {
        grid: {
          color: 'rgba(34, 34, 34, 0.08)',
        },
        ticks: {
          color: '#1b1f23',
          font: {
            size: 12,
            weight: '600',
          },
        },
      },
      y: {
        beginAtZero: true,
        grid: {
          color: 'rgba(34, 34, 34, 0.08)',
        },
        ticks: {
          color: '#1b1f23',
          callback: (value) => (barUnit ? `${value} ${barUnit}` : `${value}`),
        },
        title: {
          display: true,
          text: barUnit ? `${getVariableDisplayName(barKey)} (${barUnit})` : getVariableDisplayName(barKey),
          color: '#1b1f23',
        },
      },
      y1: {
        beginAtZero: true,
        position: 'right',
        grid: {
          drawOnChartArea: false,
        },
        ticks: {
          color: '#1b1f23',
          callback: (value) => (lineUnit ? `${value} ${lineUnit}` : `${value}`),
        },
        title: {
          display: true,
          text: lineUnit ? `${getVariableDisplayName(lineKey)} (${lineUnit})` : getVariableDisplayName(lineKey),
          color: '#1b1f23',
        },
      },
    },
    plugins: {
      legend: {
        labels: {
          color: '#1b1f23',
          font: {
            weight: '700',
          },
        },
      },
      tooltip: {
        backgroundColor: 'rgba(24, 24, 27, 0.96)',
        titleColor: '#f8fafc',
        bodyColor: '#f8fafc',
      },
    },
  };
}

export default function ChartsPanel({ refreshTick = 0 }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [lineKey, setLineKey] = useState('Temp');
  const [barKey, setBarKey] = useState('Hum');
  const [showLine, setShowLine] = useState(true);
  const [showBar, setShowBar] = useState(true);

  async function loadCharts() {
    const { desde, hasta } = getCurrentWeekBounds();

    setLoading(true);
    setError('');
    try {
      const data = await fetchWeatherRange({
        desde,
        hasta,
        limit: 1000,
      });
      setRows(data);
    } catch {
      setError('No se pudieron cargar los gráficos.');
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadCharts();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshTick]);

  const chartData = useMemo(() => {
    const datasets = [];

    if (showBar) {
      datasets.push({
        type: 'bar',
        label: `${getVariableDisplayName(barKey)} por día`,
        data: buildWeeklySeries(rows, barKey, { fillMissingWithZero: true }),
        backgroundColor: '#ff6b35',
        borderColor: '#ff6b35',
        borderRadius: 10,
        borderWidth: 1,
        yAxisID: 'y',
        order: 2,
      });
    }

    if (showLine) {
      datasets.push({
        type: 'line',
        label: `${getVariableDisplayName(lineKey)} por día`,
        data: buildWeeklySeries(rows, lineKey),
        borderColor: '#0f9d58',
        backgroundColor: '#0f9d58',
        pointBackgroundColor: '#0f9d58',
        pointBorderColor: '#ffffff',
        pointBorderWidth: 2,
        pointRadius: 4,
        tension: 0.3,
        spanGaps: true,
        yAxisID: 'y1',
        order: 1,
      });
    }

    return {
      labels: WEEKDAY_LABELS,
      datasets,
    };
  }, [barKey, lineKey, rows, showBar, showLine]);

  const chartOptions = useMemo(
    () => buildChartOptions({ lineKey, barKey }),
    [barKey, lineKey],
  );

  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <span className="panel-kicker">Gráficos</span>
          <h2>Semana actual</h2>
        </div>
        <button type="button" onClick={loadCharts} disabled={loading}>
          {loading ? 'Actualizando...' : 'Actualizar'}
        </button>
      </div>

      <div className="panel-toolbar">
        <label className="variable-inline-selector">
          Línea
          <select value={lineKey} onChange={(event) => setLineKey(event.target.value)}>
            {WEATHER_FIXED_KEYS.map((key) => (
              <option key={`line-${key}`} value={key}>
                {getVariableDisplayName(key)}
              </option>
            ))}
          </select>
        </label>
        <label className="variable-inline-selector">
          Barras
          <select value={barKey} onChange={(event) => setBarKey(event.target.value)}>
            {WEATHER_FIXED_KEYS.map((key) => (
              <option key={`bar-${key}`} value={key}>
                {getVariableDisplayName(key)}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          className={showLine ? 'toggle-button is-active' : 'toggle-button'}
          onClick={() => setShowLine((prev) => !prev)}
        >
          {showLine ? 'Ocultar línea' : 'Mostrar línea'}
        </button>
        <button
          type="button"
          className={showBar ? 'toggle-button is-active' : 'toggle-button'}
          onClick={() => setShowBar((prev) => !prev)}
        >
          {showBar ? 'Ocultar barras' : 'Mostrar barras'}
        </button>
      </div>

      {error && <p className="error">{error}</p>}
      {loading && !error && <p className="muted">Cargando semana actual…</p>}
      {!loading && !error && !showLine && !showBar && (
        <p className="muted">Activa al menos una serie para visualizar el gráfico.</p>
      )}

      <div className="chart-single-wrap">
        <div className="chart-canvas-shell">
          <Chart type="bar" data={chartData} options={chartOptions} />
        </div>
      </div>
    </section>
  );
}
