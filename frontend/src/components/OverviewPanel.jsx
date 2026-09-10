import { formatDateTime } from '../utils/dateTime';
import { Skeleton } from 'boneyard-js/react';
import {
  WEATHER_FIXED_KEYS,
  formatWeatherValue,
  getVariableDisplayName,
  scaleWeatherValue,
} from '../utils/weatherVariables';
import StatusState from './StatusState';
import { getAgronomicTone } from '../config/agronomicThresholds';

function SummaryCard({ weatherKey, value, updatedAt, loading }) {
  const tone = getAgronomicTone(weatherKey, scaleWeatherValue(weatherKey, value));
  const loadingFallback = (
    <>
      <span className="skeleton skeleton-value" />
      <span className="skeleton skeleton-line" />
    </>
  );

  return (
    <article className={`summary-card is-${tone}`}>
      <span className="summary-label">{getVariableDisplayName(weatherKey)}</span>
      <Skeleton
        name={`weather-summary-${weatherKey}`}
        loading={loading}
        animate="shimmer"
        transition={200}
        fallback={loadingFallback}
        fixture={(
          <>
            <strong className="summary-value">18.2</strong>
            <span className="summary-meta">Actualizado: 01/01/2026 12:00</span>
          </>
        )}
      >
        <>
          <strong className="summary-value">{formatWeatherValue(weatherKey, value)}</strong>
          <span className="summary-meta">
            {updatedAt ? `Actualizado: ${formatDateTime(updatedAt)}` : 'Sin lectura reciente'}
          </span>
        </>
      </Skeleton>
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
  const hasRecentReading = Boolean(readingTime) && status.toneClass === 'is-ok';

  return (
    <section className="overview-stack">
      <article className="panel overview-hero">
        <div className="overview-copy">
          <div className="eyebrow">Panel meteorológico</div>
          <h1>Estado actual de la estación</h1>
          <p>Lecturas en vivo, gráficos, histórico y resumen operativo.</p>
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

      {!loading && !hasRecentReading && (
        <StatusState
          title="Estado de la estación"
          message="Sin lectura reciente. Revisa la conexión y vuelve a intentarlo."
        />
      )}
    </section>
  );
}
