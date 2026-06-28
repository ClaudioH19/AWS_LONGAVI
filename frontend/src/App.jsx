import { useEffect, useState } from 'react';
import ChartsPanel from './components/ChartsPanel';
import DataTable from './components/DataTable';
import OverviewPanel from './components/OverviewPanel';
import { fetchHealth, fetchLatest } from './api/weatherApi';
import { formatDateTime } from './utils/dateTime';
import './App.css';

function App() {
  const [refreshTick, setRefreshTick] = useState(0);
  const [health, setHealth] = useState({
    ok: false,
    total: '-',
    ultimo: '',
    serverTimeChile: '',
    dbPath: '',
    dbSizeBytes: 0,
  });
  const [latest, setLatest] = useState(null);

  useEffect(() => {
    let active = true;

    async function loadDashboard() {
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
          dbPath: data.db_path || '',
          dbSizeBytes: data.db_size_bytes ?? 0,
        });
      } else {
        setHealth((prev) => ({ ...prev, ok: false }));
      }

      if (latestResult.status === 'fulfilled') {
        setLatest(latestResult.value);
      } else {
        setLatest(null);
      }
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

  return (
    <div className="app-shell">
      <header className="top-nav">
        <div className="brand">
          BIOVISION
          <span>Panel de estacion meteorologica</span>
        </div>
        <div className="nav-status">
          <span className={`status-pill ${health.ok ? 'is-ok' : 'is-error'}`}>
            {health.ok ? 'Online' : 'Offline'}
          </span>
          <span className="nav-time">{formatDateTime(health.serverTimeChile)}</span>
        </div>
      </header>

      <section id="panel" className="dashboard-section">
        <main>
          <OverviewPanel health={health} latest={latest} />
          <ChartsPanel refreshTick={refreshTick} />
          <DataTable refreshTick={refreshTick} />
        </main>
      </section>

      <footer className="site-footer">
        <span>Historial persistente en SQLite y visualizacion operativa del estado actual.</span>
      </footer>
    </div>
  );
}

export default App;
