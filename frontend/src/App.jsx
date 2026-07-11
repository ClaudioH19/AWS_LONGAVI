import { useEffect, useMemo, useState } from 'react';
import { io } from 'socket.io-client';
import ChartsPanel from './components/ChartsPanel';
import DataTable from './components/DataTable';
import OverviewPanel from './components/OverviewPanel';
import { fetchHealth, fetchLatest } from './api/weatherApi';
import { formatDateTime, parseDateTimeAsLocal } from './utils/dateTime';
import './App.css';

const HEALTH_POLL_INTERVAL_MS = 60000;
const RANGE_RECONCILE_EVERY_POLLS = 5;

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
  const [liveReading, setLiveReading] = useState(null);
  const [dashboardLoading, setDashboardLoading] = useState(true);

  useEffect(() => {
    let active = true;
    let pollCount = 0;

    async function loadDashboard({ initial = false } = {}) {
      if (initial) setDashboardLoading(true);
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

      if (initial) setDashboardLoading(false);
    }

    loadDashboard({ initial: true });
    const timer = setInterval(() => {
      loadDashboard();
      pollCount += 1;
      if (pollCount % RANGE_RECONCILE_EVERY_POLLS === 0) {
        setRefreshTick((prev) => prev + 1);
      }
    }, HEALTH_POLL_INTERVAL_MS);

    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    const socket = io('/', {
      path: '/socket.io',
      transports: ['websocket', 'polling'],
      reconnection: true,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 10000,
    });

    socket.on('connect', () => {
      // Reconciles any readings published while the dashboard was disconnected.
      setRefreshTick((current) => current + 1);
    });

    socket.on('weather:reading', (event) => {
      const reading = event?.reading;
      if (!reading) return;

      const receivedAt = event.last_received_at || reading.received_at || '';
      setLatest(reading);
      setLiveReading(reading);
      setHealth((previous) => ({
        ...previous,
        ok: true,
        ultimo: receivedAt || previous.ultimo,
        serverTimeChile: event.server_time_chile || previous.serverTimeChile,
      }));
    });

    return () => socket.close();
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
            <ChartsPanel refreshTick={refreshTick} status={status} liveReading={liveReading} />
          ) : (
            <DataTable refreshTick={refreshTick} liveReading={liveReading} />
          )}
        </main>
      </section>
    </div>
  );
}

export default App;
