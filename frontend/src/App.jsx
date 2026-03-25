import { useEffect, useState } from 'react';
import ChartsPanel from './components/ChartsPanel';
import DataTable from './components/DataTable';
import { fetchHealth } from './api/weatherApi';
import './App.css';

function App() {
  const [refreshTick, setRefreshTick] = useState(0);
  const [health, setHealth] = useState({
    ok: false,
    total: '-',
    ultimo: '-',
    serverTime: '-',
  });

  useEffect(() => {
    let active = true;

    async function loadHealth() {
      try {
        const data = await fetchHealth();
        if (!active) return;
        setHealth({
          ok: true,
          total: data.db_total_registros ?? '-',
          ultimo: data.ultimo_registro ?? '-',
          serverTime: data.server_time_utc?.split('.')?.[0] ?? '-',
        });
      } catch {
        if (!active) return;
        setHealth((prev) => ({ ...prev, ok: false }));
      }
    }

    loadHealth();
    const timer = setInterval(() => {
      loadHealth();
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
        <div className="brand">BIOVISION <span>Estación Meteorológica</span></div>
        <button type="button" className="cta-btn">Panel Operativo</button>
      </header>

      <section id="panel" className="dashboard-section">
        <main>
          <div className="status-bar">
            <div className="stat">
              <div className={`dot pulse ${health.ok ? 'ok' : 'error'}`} />
              <span className="stat-label">Server</span>
              <span className="stat-value">{health.ok ? 'OK' : 'ERROR'}</span>
            </div>
            <div className="stat">
              <span className="stat-label">Ultimo dato</span>
              <span className="stat-value">{health.ultimo}</span>
            </div>
            <div className="stat">
              <span className="stat-label">Total registros</span>
              <span className="stat-value">{health.total}</span>
            </div>
            <div className="stat">
              <span className="stat-label">UTC</span>
              <span className="stat-value">{health.serverTime}</span>
            </div>
          </div>
          <ChartsPanel />
          <DataTable refreshTick={refreshTick} />
        </main>
      </section>

      <footer className="site-footer">
        <span>BIOVISION Autopilot</span>
        <span>Dashboard operativo de la estación meteorológica</span>
      </footer>
    </div>
  );
}

export default App;
