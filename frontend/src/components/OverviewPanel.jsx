import { formatDateTime } from '../utils/dateTime';
import {
  WEATHER_FIXED_KEYS,
  formatWeatherValue,
  getVariableDisplayName,
} from '../utils/weatherVariables';

const CARD_TONES = {
  Temp: 'is-temp',
  Hum: 'is-hum',
  Vel: 'is-vel',
  Dir: 'is-dir',
  Precip: 'is-precip',
  Rad: 'is-rad',
};

function SummaryCard({ weatherKey, value, updatedAt, loading }) {
  return (
    <article className={`summary-card ${CARD_TONES[weatherKey] || ''}`}>
      <span className="summary-label">{getVariableDisplayName(weatherKey)}</span>
      {loading ? (
        <>
          <span className="skeleton skeleton-value" />
          <span className="skeleton skeleton-line" />
        </>
      ) : (
        <>
          <strong className="summary-value">{formatWeatherValue(weatherKey, value)}</strong>
          <span className="summary-meta">
            {updatedAt ? `Actualizado: ${formatDateTime(updatedAt)}` : 'Sin lectura reciente'}
          </span>
        </>
      )}
    </article>
  );
}

export default function OverviewPanel({ latest, status, loading = false }) {
  // Solo received_at: Timestamp lo genera el firmware del equipo y puede
  // venir corrupto (ej. "2026-07-182 09:58:24"), lo que mostraría una fecha
  // rota o sin formatear. received_at es la hora en que el servidor recibió
  // el dato y es la única fuente confiable. Consistente con ChartsPanel.
  const readingTime = latest?.received_at || '';
  const deviceName = latest?.DeviceID || 'Sin equipo';

  return (
    <section className="overview-stack">
      <article className="panel overview-hero">
        <div className="overview-copy">
          <div className="eyebrow">Panel meteorológico</div>
          <h1>Estado actual de la estación</h1>
          <p>Lecturas en vivo, conmutador de vistas y resumen operativo.</p>
        </div>

        <div className="health-chips">
          <span className={`health-chip ${status.toneClass}`}>{status.label}</span>
          <span className="health-chip">Equipo: {deviceName}</span>
          <span className="health-chip">
            {readingTime ? `Último dato: ${formatDateTime(readingTime)}` : 'Sin datos reportados'}
          </span>
        </div>
      </article>

      <div className="summary-grid">
        {WEATHER_FIXED_KEYS.map((key) => (
          <SummaryCard
            key={key}
            weatherKey={key}
            value={latest?.[key]}
            updatedAt={readingTime}
            loading={loading}
          />
        ))}
      </div>
    </section>
  );
}
