import { useMemo, useState } from 'react';
import { buildVariableQualitySummaries } from '../utils/dataQuality';

function formatDelta(delta) {
  if (!Number.isFinite(delta)) return 'Sin referencia histórica';
  if (delta === 0) return 'Sin cambio';
  return `${delta > 0 ? '+' : ''}${delta} pts de calidad`;
}

export default function DataQualityAlerts({ rows, days }) {
  const [open, setOpen] = useState(false);
  const alerts = useMemo(
    () => buildVariableQualitySummaries(rows, days).flatMap((summary) => (
      summary.problems.map((problem) => ({
        id: `${summary.key}-${problem.key}`,
        variable: summary.name,
        ...problem,
      }))
    )),
    [days, rows],
  );

  if (!alerts.length) return null;

  const criticalCount = alerts.filter(({ severity }) => severity === 'poor').length;

  return (
    <div className="quality-alerts">
      <button
        type="button"
        className={criticalCount ? 'quality-alert-trigger is-critical' : 'quality-alert-trigger'}
        aria-expanded={open}
        aria-controls="quality-alert-popover"
        onClick={() => setOpen((current) => !current)}
      >
        <span className="quality-alert-icon" aria-hidden="true">!</span>
        <span>Calidad de datos</span>
        <span className="quality-alert-count">{alerts.length}</span>
      </button>

      {open && (
        <div id="quality-alert-popover" className="quality-alert-popover" role="dialog" aria-label="Alertas de calidad de datos">
          <div className="quality-alert-heading">
            <div>
              <strong>{alerts.length} {alerts.length === 1 ? 'alerta detectada' : 'alertas detectadas'}</strong>
              <span>7 días recientes frente a los 23 anteriores</span>
            </div>
            <button type="button" aria-label="Cerrar alertas" onClick={() => setOpen(false)}>×</button>
          </div>

          <div className="quality-alert-list">
            {alerts.map((alert) => (
              <article className={`quality-alert-item is-${alert.severity}`} key={alert.id}>
                <div className="quality-alert-item-heading">
                  <strong>{alert.variable}</strong>
                  <span>{alert.value}</span>
                </div>
                <span className="quality-alert-problem">{alert.label}</span>
                <span className={`quality-alert-trend is-${alert.trend.status}`}>
                  {alert.trend.label} · {formatDelta(alert.trend.delta)}
                </span>
              </article>
            ))}
          </div>

          <p>Diagnóstico automático; no reemplaza la calibración del sensor.</p>
        </div>
      )}
    </div>
  );
}
