import { formatDateTime } from '../utils/dateTime';
import {
  WEATHER_FIXED_KEYS,
  formatWeatherValue,
  getVariableDisplayName,
} from '../utils/weatherVariables';

function formatDbSize(sizeInBytes) {
  const numericSize = Number(sizeInBytes);
  if (!Number.isFinite(numericSize) || numericSize <= 0) return '0 B';

  const units = ['B', 'KB', 'MB', 'GB'];
  let value = numericSize;
  let unitIndex = 0;

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }

  const decimals = value >= 10 || unitIndex === 0 ? 0 : 1;
  return `${value.toFixed(decimals)} ${units[unitIndex]}`;
}

function SummaryCard({ label, value, meta }) {
  return (
    <article className="summary-card">
      <span className="summary-label">{label}</span>
      <strong className="summary-value">{value}</strong>
      <span className="summary-meta">{meta}</span>
    </article>
  );
}

export default function OverviewPanel({ health, latest }) {
  const readingTime = latest?.received_at || latest?.Timestamp || '';
  const deviceName = latest?.DeviceID || 'Sin dispositivo';

  return (
    <section className="overview-stack">
      <div className="overview-grid">
        <article className="panel overview-hero">
          <div className="eyebrow">Panel meteorologico</div>
          <h1>Monitoreo en tiempo real de la estacion</h1>
          <p>
            Resumen del dato actual, tendencias recientes y respaldo historico en una sola
            vista operativa.
          </p>
          <div className="health-chips">
            <span className={`health-chip ${health.ok ? 'is-ok' : 'is-error'}`}>
              {health.ok ? 'Servidor activo' : 'Servidor con error'}
            </span>
            <span className="health-chip">SQLite persistente</span>
            <span className="health-chip">Registros: {health.total}</span>
            <span className="health-chip">Equipo: {deviceName}</span>
          </div>
        </article>

        <article className="panel storage-panel">
          <span className="panel-kicker">Persistencia</span>
          <strong className="storage-title">La data queda almacenada en SQLite</strong>
          <div className="storage-stats">
            <div>
              <span className="storage-label">Ultimo dato</span>
              <strong>{formatDateTime(health.ultimo)}</strong>
            </div>
            <div>
              <span className="storage-label">Tamano DB</span>
              <strong>{formatDbSize(health.dbSizeBytes)}</strong>
            </div>
            <div>
              <span className="storage-label">Archivo</span>
              <strong className="storage-path">{health.dbPath || '--'}</strong>
            </div>
          </div>
        </article>
      </div>

      <div className="summary-grid">
        {WEATHER_FIXED_KEYS.map((key) => (
          <SummaryCard
            key={key}
            label={getVariableDisplayName(key)}
            value={formatWeatherValue(key, latest?.[key])}
            meta={readingTime ? `Actualizado: ${formatDateTime(readingTime)}` : 'Sin lecturas aun'}
          />
        ))}
      </div>
    </section>
  );
}
