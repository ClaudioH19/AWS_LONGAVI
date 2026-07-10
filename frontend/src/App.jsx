import { useEffect, useMemo, useState } from 'react';
import ChartsPanel from './components/ChartsPanel';
import DataTable from './components/DataTable';
import OverviewPanel from './components/OverviewPanel';
import { fetchHealth, fetchLatest } from './api/weatherApi';
import { formatDateTime, parseDateTimeAsLocal } from './utils/dateTime';
import './App.css';

function getStatusModel({ healthOk, latestTimestamp, serverTimestamp }) {
  if (!healthOk) {
    return { label: 'Con problemas', toneClass: 'is-error' };
  }

  if (!latestTimestamp || !serverTimestamp) {
    return { label: 'Con interrupciones', toneClass: 'is-warning' };
  }

  const latestDate = parseDateTimeAsLocal(latestTimestamp);
  const serverDate = parseDateTimeAsLocal(serverTimestamp);
  if (!latestDate || !serverDate) {
    return { label: 'Con interrupciones', toneClass: 'is-warning' };
  }

  const hoursWithoutData = (serverDate.getTime() - latestDate.getTime()) / (1000 * 60 * 60);
  if (hoursWithoutData > 3) {
    return { label: 'Con interrupciones', toneClass: 'is-warning' };
  }

  return { label: 'Online', toneClass: 'is-ok' };
}

function App() {
  const [refreshTick, setRefreshTick] = useState(0);
  const [activeView, setActiveView] = useState('charts');
  const [health, setHealth] = useState({
    ok: false,
    total: '-',
    ultimo: '',
    serverTimeChile: '',
  });
  const [latest, setLatest] = useState(null);
  const [dashboardLoading, setDashboardLoading] = useState(true);

  useEffect(() => {
    let active = true;

    async function loadDashboard() {
      setDashboardLoading(true);
      const [healthResult, latestResult] = await Promise.allSettled([
        fetchHealth(),
        fetchLatest(),
      ]);

      if (!active) {
        return;
      }

      if (healthResult.status === 'fulfilled') {
        const data = healthResult.value;
        setHealth({
          ok: true,
          total: data.db_total_registros ?? '-',
          ultimo: data.ultimo_registro || '',
          serverTimeChile: data.server_time_chile || data.server_time_utc || '',
        });
      } else {
        setHealth((prev) => ({ ...prev, ok: false }));
      }

      if (latestResult.status === 'fulfilled') {
        setLatest(latestResult.value);
      } else {
        setLatest(null);
      }

      setDashboardLoading(false);
    }

    loadDashboard();
    const timer = setInterval(() => {
      loadDashboard();
      setRefreshTick((prev) => prev + 1);
    }, 30000);

    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  const latestTimestamp = latest?.received_at || latest?.Timestamp || health.ultimo;
  const status = useMemo(
    () => getStatusModel({
      healthOk: health.ok,
      latestTimestamp,
      serverTimestamp: health.serverTimeChile,
    }),
    [health.ok, health.serverTimeChile, latestTimestamp],
  );

  return (
    <div className="app-shell">
      <header className="top-nav">
        <div className="brand">
          BIOVISION
          <span>Panel de estación meteorológica</span>
        </div>
        <div className="nav-status">
          <span className={`status-pill ${status.toneClass}`}>{status.label}</span>
          <span className="nav-time">{formatDateTime(health.serverTimeChile)}</span>
        </div>
      </header>

      <section id="panel" className="dashboard-section">
        <main>
          <OverviewPanel latest={latest} status={status} loading={dashboardLoading} />

          <nav className="panel-nav" aria-label="Cambiar vista">
            <button
              type="button"
              className={activeView === 'charts' ? 'is-active' : ''}
              onClick={() => setActiveView('charts')}
            >
              Gráficos
            </button>
            <button
              type="button"
              className={activeView === 'table' ? 'is-active' : ''}
              onClick={() => setActiveView('table')}
            >
              Tabla de datos
            </button>
          </nav>

          {activeView === 'charts' ? (
            <ChartsPanel refreshTick={refreshTick} status={status} />
          ) : (
            <DataTable refreshTick={refreshTick} />
          )}
        </main>
      </section>
    </div>
  );
}

export default App;
