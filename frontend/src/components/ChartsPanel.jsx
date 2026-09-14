import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  BarController,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from 'chart.js';
import { fetchAllWeatherRange } from '../api/weatherApi';
import {
  buildDailyWeatherStatistics,
  buildTrailingDays,
  getTrailingDaysBounds,
} from '../utils/weatherChartSeries';
import DataQualityAlerts from './DataQualityAlerts';
import DailySummaryTable from './DailySummaryTable';
import FixedWeatherCharts from './FixedWeatherCharts';
import GeneralComparisonChart from './GeneralComparisonChart';
import StatusState from './StatusState';
import { getTodayInChileDateInput } from '../utils/dateTime';

ChartJS.register(
  BarController,
  BarElement,
  CategoryScale,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
);

function useMediaQuery(query) {
  const getMatches = () => (
    typeof window !== 'undefined' ? window.matchMedia(query).matches : false
  );
  const [matches, setMatches] = useState(getMatches);

  useEffect(() => {
    const media = window.matchMedia(query);
    const updateMatch = (event) => setMatches(event.matches);
    media.addEventListener('change', updateMatch);
    return () => media.removeEventListener('change', updateMatch);
  }, [query]);

  return matches;
}

export default function ChartsPanel({
  refreshTick = 0,
  status,
  liveReading = null,
  displayMode = 'auto',
}) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [reloadToken, setReloadToken] = useState(0);
  const [loadError, setLoadError] = useState('');
  const [qualityRows, setQualityRows] = useState([]);
  const isPhone = useMediaQuery('(max-width: 767px)');
  const showGeneralChart = isPhone && displayMode !== 'tv';
  const dateAnchor = getTodayInChileDateInput();
  const days = useMemo(() => buildTrailingDays(7, dateAnchor), [dateAnchor]);
  const qualityDays = useMemo(() => buildTrailingDays(30, dateAnchor), [dateAnchor]);
  const dailyStatistics = useMemo(
    () => buildDailyWeatherStatistics(rows, days),
    [days, rows],
  );

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
        const data = await fetchAllWeatherRange(chartBounds, { maxRows: 15000 });
        if (!active) return;
        setRows(data);

        qualityTimer = window.setTimeout(async () => {
          try {
            const qualityBounds = getTrailingDaysBounds(30);
            const historicalData = await fetchAllWeatherRange(qualityBounds, { maxRows: 50000 });
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

  const stationIssue = status?.toneClass && status.toneClass !== 'is-ok';
  const stationTitle = status?.toneClass === 'is-error'
    ? 'Estación fuera de línea'
    : 'Estación con interrupciones';
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
              {showGeneralChart ? (
                <GeneralComparisonChart dailyStatistics={dailyStatistics} />
              ) : (
                <FixedWeatherCharts dailyStatistics={dailyStatistics} />
              )}
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
