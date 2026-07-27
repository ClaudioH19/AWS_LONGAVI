import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  BarController,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from 'chart.js';
import { Chart } from 'react-chartjs-2';
import { fetchWeatherRange } from '../api/weatherApi';
import DataQualityAlerts from './DataQualityAlerts';
import DailySummaryTable from './DailySummaryTable';
import StatusState from './StatusState';
import { getTodayInChileDateInput, parseDateTimeParts } from '../utils/dateTime';
import {
  WEATHER_FIXED_KEYS,
  getUnitForKey,
  getVariableDisplayName,
  scaleWeatherValue,
} from '../utils/weatherVariables';

const WEEKDAY_LABELS = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom'];
const COLORS = {
  grid: '#E5E7EB',
  text: '#0B2D1B',
  tooltip: '#1F2937',
};
const VARIABLE_COLORS = {
  Temp: '#C65D2E',
  Hum: '#0F766E',
  Vel: '#2F855A',
  Dir: '#475569',
  Precip: '#2563EB',
  Rad: '#D97706',
};
const BAR_COLORS = {
  Temp: { background: '#FEF3C7', border: '#D97706' },
  Hum: { background: '#DBEAFE', border: '#2563EB' },
  Vel: { background: '#DCFCE7', border: '#16A34A' },
  Dir: { background: '#F3F4F6', border: '#64748B' },
  Precip: { background: '#E0F2FE', border: '#0284C7' },
  Rad: { background: '#FEF3C7', border: '#CA8A04' },
};

ChartJS.register(
  BarController,
  BarElement,
  CategoryScale,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
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

function getTrailingDaysBounds(totalDays = 7, dateInput = getTodayInChileDateInput()) {
  const today = parseDateParts(dateInput);
  if (!today) return { desde: '', hasta: '' };

  const todayDate = new Date(Date.UTC(today.year, today.month - 1, today.day));
  const firstDay = new Date(todayDate);
  firstDay.setUTCDate(todayDate.getUTCDate() - (totalDays - 1));

  return {
    desde: toDateInputFromUTC(firstDay),
    hasta: toDateInputFromUTC(todayDate),
  };
}

function getDateInputFromDateTime(value) {
  const parts = parseDateTimeParts(value);
  if (!parts) return null;
  return toDateInputFromUTC(new Date(Date.UTC(parts.year, parts.month - 1, parts.day)));
}

function buildTrailingDays(totalDays = 7, dateInput = getTodayInChileDateInput()) {
  const start = parseDateParts(getTrailingDaysBounds(totalDays, dateInput).desde);
  if (!start) return [];

  const firstDay = new Date(Date.UTC(start.year, start.month - 1, start.day));
  return Array.from({ length: totalDays }, (_, index) => {
    const date = new Date(firstDay);
    date.setUTCDate(firstDay.getUTCDate() + index);
    const weekdayIndex = (date.getUTCDay() + 6) % 7;
    return {
      key: toDateInputFromUTC(date),
      label: `${WEEKDAY_LABELS[weekdayIndex]} ${String(date.getUTCDate()).padStart(2, '0')}/${String(date.getUTCMonth() + 1).padStart(2, '0')}`,
    };
  });
}

function buildDailySeries(rows, days, key, aggregation = 'average') {
  const totals = new Map(days.map(({ key: day }) => [day, 0]));
  const counts = new Map(days.map(({ key: day }) => [day, 0]));

  rows.forEach((row) => {
    const day = getDateInputFromDateTime(row.received_at);
    const value = scaleWeatherValue(key, row[key]);
    if (!totals.has(day) || value === null) return;

    totals.set(day, totals.get(day) + value);
    counts.set(day, counts.get(day) + 1);
  });

  return days.map(({ key: day }) => {
    const count = counts.get(day);
    if (!count) return null;
    const total = totals.get(day);
    return Number((aggregation === 'sum' ? total : total / count).toFixed(2));
  });
}


function formatTooltipValue(value) {
  if (!Number.isFinite(Number(value))) return '--';
  return Number(value).toLocaleString('es-CL', {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });
}

function buildOptions({ leftAxisTitle, rightAxisTitle = '' }) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: {
        grid: { color: COLORS.grid },
        ticks: { color: COLORS.text, font: { size: 11, weight: '600' }, maxRotation: 0 },
      },
      y: {
        beginAtZero: true,
        grid: { color: COLORS.grid },
        ticks: { color: COLORS.text },
        title: { display: true, text: leftAxisTitle, color: COLORS.text },
      },
      ...(rightAxisTitle ? {
        y1: {
          beginAtZero: true,
          position: 'right',
          grid: { drawOnChartArea: false },
          ticks: { color: COLORS.text },
          title: { display: true, text: rightAxisTitle, color: COLORS.text },
        },
      } : {}),
    },
    plugins: {
      legend: {
        position: 'top',
        align: 'end',
        labels: { color: COLORS.text, boxWidth: 10, boxHeight: 10, padding: 12, font: { weight: '700' } },
      },
      tooltip: {
        backgroundColor: COLORS.tooltip,
        titleColor: '#FFFFFF',
        bodyColor: '#FFFFFF',
        borderColor: '#111827',
        borderWidth: 1,
        cornerRadius: 4,
        padding: 10,
        caretPadding: 8,
        callbacks: {
          title: (contexts) => (contexts.length ? `Fecha: ${contexts[0].label}, 00:00-23:59` : ''),
          label: (context) => `${context.dataset.label}: ${formatTooltipValue(context.parsed.y)}`,
        },
      },
    },
  };
}

function getSeriesLabel(key) {
  const unit = getUnitForKey(key);
  const name = getVariableDisplayName(key);
  return unit ? `${name} (${unit})` : name;
}

function getComparisonQuestion(lineKey, barKey) {
  return `¿Cómo se relacionan ${getVariableDisplayName(lineKey).toLowerCase()} y ${getVariableDisplayName(barKey).toLowerCase()}?`;
}

function getBarColors(key) {
  return BAR_COLORS[key] || BAR_COLORS.Dir;
}

export default function ChartsPanel({ refreshTick = 0, status, liveReading = null }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [reloadToken, setReloadToken] = useState(0);
  const [loadError, setLoadError] = useState('');
  const [lineKey, setLineKey] = useState('Temp');
  const [barKey, setBarKey] = useState('Hum');
  const [qualityRows, setQualityRows] = useState([]);
  const dateAnchor = getTodayInChileDateInput();
  const days = useMemo(() => buildTrailingDays(7, dateAnchor), [dateAnchor]);
  const qualityDays = useMemo(() => buildTrailingDays(30, dateAnchor), [dateAnchor]);
  const labels = useMemo(() => days.map(({ label }) => label), [days]);

  const loadCharts = useCallback(() => {
    setReloadToken((current) => current + 1);
  }, []);

  useEffect(() => {
    let active = true;
    let qualityTimer = null;

    async function loadRealReadings() {
      setLoading(true);
      setLoadError('');
      setQualityRows([]);

      try {
        const chartBounds = getTrailingDaysBounds(7);
        const data = await fetchWeatherRange({ ...chartBounds, limit: 5000 });
        if (!active) return;
        setRows(data);

        qualityTimer = window.setTimeout(async () => {
          try {
            const qualityBounds = getTrailingDaysBounds(30);
            const historicalData = await fetchWeatherRange({ ...qualityBounds, limit: 50000 });
            if (active) setQualityRows(historicalData);
          } catch {
            if (active) setQualityRows(data);
          }
        }, 800);
      } catch {
        if (!active) return;
        setRows([]);
        setQualityRows([]);
        setLoadError('No se pudieron cargar las lecturas reales de los últimos 7 días.');
      } finally {
        if (active) setLoading(false);
      }
    }

    loadRealReadings();

    return () => {
      active = false;
      if (qualityTimer !== null) window.clearTimeout(qualityTimer);
    };
  }, [refreshTick, reloadToken]);

  useEffect(() => {
    if (!liveReading) return;

    const addLiveReading = (previous) => {
      const alreadyPresent = previous.some((row) => (
        (liveReading.id && row.id === liveReading.id)
        || row.received_at === liveReading.received_at
      ));
      return alreadyPresent ? previous : [liveReading, ...previous];
    };

    setRows(addLiveReading);
    setQualityRows(addLiveReading);
  }, [liveReading]);

  const baseChartData = useMemo(() => ({
    labels,
    datasets: [
      {
        type: 'bar',
        label: getSeriesLabel(barKey),
        data: buildDailySeries(rows, days, barKey, barKey === 'Precip' ? 'sum' : 'average'),
        backgroundColor: getBarColors(barKey).background,
        borderColor: getBarColors(barKey).border,
        borderRadius: 4,
        borderWidth: 1,
        maxBarThickness: 60,
        categoryPercentage: 0.9,
        barPercentage: 0.95,
        yAxisID: 'y1',
        order: 3,
      },
      {
        type: 'line',
        label: getSeriesLabel(lineKey),
        data: buildDailySeries(rows, days, lineKey),
        borderColor: VARIABLE_COLORS[lineKey] || COLORS.text,
        backgroundColor: VARIABLE_COLORS[lineKey] || COLORS.text,
        pointBackgroundColor: VARIABLE_COLORS[lineKey] || COLORS.text,
        pointBorderColor: '#FFFFFF',
        pointBorderWidth: 2,
        pointRadius: 3,
        tension: 0.3,
        yAxisID: 'y',
        order: 1,
      },
    ],
  }), [barKey, days, labels, lineKey, rows]);
  const baseOptions = useMemo(
    () => buildOptions({ leftAxisTitle: getSeriesLabel(lineKey), rightAxisTitle: getSeriesLabel(barKey) }),
    [barKey, lineKey],
  );
  const stationIssue = status?.toneClass && status.toneClass !== 'is-ok';
  const stationTitle = status?.toneClass === 'is-error' ? 'Estación fuera de línea' : 'Estación con interrupciones';
  const stationMessage = status?.toneClass === 'is-error'
    ? 'No se pudo verificar la conexión con la estación.'
    : 'La estación no ha entregado una lectura reciente.';

  return (
    <section className="panel trends-panel">
      <div className="section-heading">
        <div>
          <span className="panel-kicker">Tendencias</span>
          <h2>Evolución meteorológica semanal</h2>
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
              <option key={`line-${key}`} value={key}>{getVariableDisplayName(key)}</option>
            ))}
          </select>
        </label>
        <label className="variable-inline-selector">
          Barras
          <select value={barKey} onChange={(event) => setBarKey(event.target.value)}>
            {WEATHER_FIXED_KEYS.map((key) => (
              <option key={`bar-${key}`} value={key}>{getVariableDisplayName(key)}</option>
            ))}
          </select>
        </label>
      </div>

      <div className="chart-workspace">
        <div className={stationIssue ? 'chart-visuals is-offline' : 'chart-visuals'}>
          {loading && (
            <div className="chart-canvas-shell chart-loading" aria-label="Cargando gráficos">
              <span className="spinner" />
              <div className="skeleton-chart"><span /><span /><span /><span /><span /></div>
            </div>
          )}
          {!loading && loadError && (
            <StatusState
              title="No se pudieron cargar los datos"
              message={loadError}
              onRetry={loadCharts}
            />
          )}
          {!loading && !loadError && rows.length === 0 && (
            <StatusState
              title="No hay datos disponibles"
              message="Aún no hay lecturas para los últimos 7 días."
              onRetry={loadCharts}
            />
          )}
          {!loading && !loadError && rows.length > 0 && (
            <div className="trend-stack">
              <section className="trend-chart" aria-labelledby="base-chart-title">
                <h3 id="base-chart-title">{getComparisonQuestion(lineKey, barKey)}</h3>
                <div className="chart-canvas-shell">
                  <Chart type="bar" data={baseChartData} options={baseOptions} />
                </div>
              </section>
              <DailySummaryTable rows={rows} />
            </div>
          )}
        </div>
        {!loading && !loadError && rows.length > 0 && qualityRows.length > 0 && !stationIssue && (
          <DataQualityAlerts rows={qualityRows} days={qualityDays} />
        )}
        {stationIssue && (
          <div className="chart-offline-overlay">
            <StatusState
              title={stationTitle}
              message={stationMessage}
              actionLabel="Reintentar conexión"
              onRetry={loadCharts}
            />
          </div>
        )}
      </div>
    </section>
  );
}
