import { useEffect, useMemo, useState } from 'react';
import {
  BarElement,
  BarController,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineElement,
  LineController,
  LinearScale,
  PointElement,
  Title,
  Tooltip,
} from 'chart.js';
import { Chart } from 'react-chartjs-2';
import { fetchWeatherRange } from '../api/weatherApi';
import StatusState from './StatusState';
import { getTodayInChileDateInput, parseDateTimeParts } from '../utils/dateTime';
import {
  WEATHER_FIXED_KEYS,
  getVariableDisplayName,
  getUnitForKey,
  scaleWeatherValue,
} from '../utils/weatherVariables';

const WEEKDAY_LABELS = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom'];

ChartJS.register(
  BarController,
  LineController,
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

// Solo received_at: el campo Timestamp lo genera el firmware del equipo y
// puede venir corrupto (ej. "2026-07-182 09:58:24", con el día mal formado),
// lo que rompía el parseo y desalineaba el día de la semana en el gráfico.
// received_at es la hora en que el servidor recibió el dato (ya en hora de
// Chile) y no depende del firmware del equipo, así que es la única fuente
// confiable para agrupar por día.
function getTimestampForRow(row) {
  return row.received_at || '';
}

// Reusa el mismo parser central que usa formatDateTime/formatDateTimeShort,
// para que el día de la semana se calcule siempre a partir de los mismos
// año/mes/día que se muestran en el resto de la UI, sin duplicar el regex
// ni arriesgar que las dos implementaciones diverjan.
function getWeekdayIndex(value) {
  const parts = parseDateTimeParts(value);
  if (!parts) return null;

  const date = new Date(Date.UTC(parts.year, parts.month - 1, parts.day));
  const jsDay = date.getUTCDay(); // 0 = Domingo ... 6 = Sábado
  return (jsDay + 6) % 7; // 0 = Lunes ... 6 = Domingo
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

const BRAND = {
  emerald: '#13B76C',
  forest: '#082D1F',
  text: '#0B2D1B',
  grid: 'rgba(8, 45, 31, 0.09)',
  tooltipBg: 'rgba(8, 45, 31, 0.96)',
  tooltipText: '#FFFFFF',
};

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
          color: BRAND.grid,
        },
        ticks: {
          color: BRAND.text,
          font: {
            size: 12,
            weight: '600',
          },
        },
      },
      y: {
        beginAtZero: true,
        grid: {
          color: BRAND.grid,
        },
        ticks: {
          color: BRAND.text,
          callback: (value) => (barUnit ? `${value} ${barUnit}` : `${value}`),
        },
        title: {
          display: true,
          text: barUnit ? `${getVariableDisplayName(barKey)} (${barUnit})` : getVariableDisplayName(barKey),
          color: BRAND.text,
        },
      },
      y1: {
        beginAtZero: true,
        position: 'right',
        grid: {
          drawOnChartArea: false,
        },
        ticks: {
          color: BRAND.text,
          callback: (value) => (lineUnit ? `${value} ${lineUnit}` : `${value}`),
        },
        title: {
          display: true,
          text: lineUnit ? `${getVariableDisplayName(lineKey)} (${lineUnit})` : getVariableDisplayName(lineKey),
          color: BRAND.text,
        },
      },
    },
    plugins: {
      legend: {
        labels: {
          color: BRAND.text,
          font: {
            weight: '700',
          },
        },
      },
      tooltip: {
        backgroundColor: BRAND.tooltipBg,
        titleColor: BRAND.tooltipText,
        bodyColor: BRAND.tooltipText,
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
  }, [refreshTick]);

  const chartData = useMemo(() => {
    const datasets = [];

    if (showBar) {
      datasets.push({
        type: 'bar',
        label: `${getVariableDisplayName(barKey)} por día`,
        data: buildWeeklySeries(rows, barKey, { fillMissingWithZero: true }),
        backgroundColor: BRAND.emerald,
        borderColor: BRAND.emerald,
        borderRadius: 6,
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
        borderColor: BRAND.forest,
        backgroundColor: BRAND.forest,
        pointBackgroundColor: BRAND.forest,
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

      {error && (
        <StatusState
          title="Estación fuera de línea"
          message="No se pudieron cargar los gráficos desde el backend."
          onRetry={loadCharts}
        />
      )}
      {loading && !error && (
        <div className="chart-canvas-shell chart-loading" aria-label="Cargando gráficos">
          <span className="spinner" />
          <div className="skeleton-chart">
            <span />
            <span />
            <span />
            <span />
            <span />
          </div>
        </div>
      )}
      {!loading && !error && !showLine && !showBar && (
        <StatusState
          title="No hay series activas"
          message="Activa al menos una serie para visualizar el gráfico."
          actionLabel="Activar series"
          onRetry={() => {
            setShowLine(true);
            setShowBar(true);
          }}
        />
      )}
      {!loading && !error && rows.length === 0 && (showLine || showBar) && (
        <StatusState
          title="No hay datos disponibles"
          message="No existen lecturas para la semana actual."
          onRetry={loadCharts}
        />
      )}
      {!loading && !error && rows.length > 0 && (showLine || showBar) && (
        <div className="chart-single-wrap">
          <div className="chart-canvas-shell">
            <Chart type="bar" data={chartData} options={chartOptions} />
          </div>
        </div>
      )}
    </section>
  );
}
