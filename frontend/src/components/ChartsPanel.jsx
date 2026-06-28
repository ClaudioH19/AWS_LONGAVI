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

function getTimestampForRow(row) {
  return row.Timestamp || row.received_at || '';
}

function getDateKey(value) {
  return String(value || '').trim().slice(0, 10);
}

function formatDayLabel(dateKey) {
  const match = String(dateKey || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return dateKey;
  return `${match[3]}/${match[2]}`;
}

function toSeries(rows, key) {
  return rows
    .slice()
    .reverse()
    .map((row) => scaleWeatherValue(key, row[key]));
}

function toDailyAverageData(rows, key) {
  const grouped = new Map();

  rows.forEach((row) => {
    const timestamp = getTimestampForRow(row);
    const dateKey = getDateKey(timestamp);
    const scaledValue = scaleWeatherValue(key, row[key]);

    if (!dateKey || scaledValue === null) {
      return;
    }

    const current = grouped.get(dateKey) || { sum: 0, count: 0 };
    grouped.set(dateKey, {
      sum: current.sum + scaledValue,
      count: current.count + 1,
    });
  });

  const entries = Array.from(grouped.entries())
    .sort(([left], [right]) => left.localeCompare(right))
    .slice(-7);

  return {
    labels: entries.map(([dateKey]) => formatDayLabel(dateKey)),
    values: entries.map(([, value]) => Number((value.sum / value.count).toFixed(2))),
  };
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
          color: 'rgba(125, 145, 132, 0.14)',
        },
        ticks: {
          color: '#42514a',
          maxRotation: 0,
          autoSkip: !isBarChart,
          maxTicksLimit: isBarChart ? undefined : 10,
        },
        title: {
          display: true,
          text: isBarChart ? 'Día' : 'Fecha y hora',
          color: '#304039',
        },
      },
      y: {
        grace: '8%',
        grid: {
          color: 'rgba(125, 145, 132, 0.16)',
        },
        ticks: {
          color: '#42514a',
          callback: (value) => (unit ? `${value} ${unit}` : `${value}`),
        },
        title: {
          display: true,
          text: yTitle,
          color: '#304039',
        },
      },
    },
    plugins: {
      legend: {
        labels: {
          color: '#304039',
          font: {
            weight: '700',
          },
        },
      },
      tooltip: {
        backgroundColor: 'rgba(19, 32, 26, 0.94)',
        titleColor: '#eef6f2',
        bodyColor: '#eef6f2',
        callbacks: {
          label: (context) => {
            const value = context.parsed?.y;
            if (value === null || value === undefined) {
              return 'Sin dato';
            }
            return unit ? `${selectedLabel}: ${value.toFixed(2)} ${unit}` : `${selectedLabel}: ${value.toFixed(2)}`;
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
    limit: 240,
  });
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [lineKey, setLineKey] = useState('Temp');
  const [barKey, setBarKey] = useState('Hum');

  const lineLabels = useMemo(
    () => rows.slice().reverse().map((row) => formatDateTimeShort(getTimestampForRow(row))),
    [rows],
  );

  async function loadCharts() {
    setLoading(true);
    setError('');
    try {
      const data = await fetchWeatherRange({
        desde: filters.desde,
        hasta: filters.hasta,
        limit: filters.limit,
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

  const lineData = useMemo(() => {
    const unit = getUnitForKey(lineKey);
    const label = getVariableDisplayName(lineKey);

    return {
      labels: lineLabels,
      datasets: [
        {
          label: unit ? `${label} (${unit})` : label,
          data: toSeries(rows, lineKey),
          borderColor: '#0f766e',
          backgroundColor: 'rgba(15, 118, 110, 0.16)',
          tension: 0.28,
          borderWidth: 2.5,
          fill: true,
          spanGaps: true,
        },
      ],
    };
  }, [lineKey, lineLabels, rows]);

  const barData = useMemo(() => {
    const unit = getUnitForKey(barKey);
    const label = getVariableDisplayName(barKey);
    const dailyAverage = toDailyAverageData(rows, barKey);

    return {
      labels: dailyAverage.labels,
      datasets: [
        {
          label: unit ? `Promedio diario ${label} (${unit})` : `Promedio diario ${label}`,
          data: dailyAverage.values,
          borderColor: '#d97706',
          backgroundColor: 'rgba(217, 119, 6, 0.4)',
          borderRadius: 10,
          borderWidth: 1.5,
        },
      ],
    };
  }, [barKey, rows]);

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
          <span className="panel-kicker">Gráficos</span>
          <h2>Lecturas y promedio diario</h2>
        </div>
        <button type="button" onClick={loadCharts} disabled={loading}>
          {loading ? 'Actualizando...' : 'Actualizar'}
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
          Límite
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
      </div>

      {error && <p className="error">{error}</p>}
      {loading && !error && <p className="muted">Cargando histórico…</p>}
      {!loading && !error && rows.length === 0 && <p className="muted">No hay datos para construir los gráficos.</p>}

      <div className="chart-grid">
        <ChartCard title="Gráfico de línea" subtitle="Tendencia reciente.">
          <Line data={lineData} options={lineOptions} />
        </ChartCard>

        <ChartCard title="Gráfico de barras" subtitle="Promedio de los últimos días con datos.">
          <Bar data={barData} options={barOptions} />
        </ChartCard>
      </div>
    </section>
  );
}
