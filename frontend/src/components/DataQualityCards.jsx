import { useMemo } from 'react';
import { buildVariableQualitySummaries } from '../utils/dataQuality';

function formatDelta(delta) {
  if (!Number.isFinite(delta)) return 'Referencia no disponible';
  if (delta === 0) return 'Sin cambio';
  return `${delta > 0 ? '+' : ''}${delta} pts de calidad`;
}

export default function DataQualityCards({ rows, days }) {
  const summaries = useMemo(
    () => buildVariableQualitySummaries(rows, days),
    [days, rows],
  );
  const cards = summaries.flatMap((summary) => {
    if (!summary.problems.length) {
      return [{
        id: `${summary.key}-healthy`,
        variable: summary.name,
        problem: 'Sin problemas detectados',
        value: `${summary.score}%`,
        score: summary.score,
        severity: 'excellent',
        trend: null,
        title: `Disponibilidad ${summary.completeness}% · Rangos válidos ${summary.plausibility}% · Continuidad ${summary.continuity}% · Frescura ${summary.freshness}%`,
      }];
    }

    return summary.problems.map((problem) => ({
      id: `${summary.key}-${problem.key}`,
      variable: summary.name,
      problem: problem.label,
      value: problem.value,
      score: problem.score,
      severity: problem.severity,
      trend: problem.trend,
      title: `${problem.label}: ${problem.value}. Calidad del indicador: ${problem.score}%.`,
    }));
  });

  return (
    <section className="quality-summary" aria-labelledby="quality-summary-title">
      <div className="quality-summary-heading">
        <div>
          <span className="panel-kicker">Confiabilidad</span>
          <h3 id="quality-summary-title">Diagnóstico de calidad por problema</h3>
        </div>
        <p>Compara los últimos 7 días con los 23 anteriores; no reemplaza una calibración.</p>
      </div>

      <div className="quality-card-grid">
        {cards.map((card) => (
          <article
            className={`quality-card is-${card.severity}`}
            key={card.id}
            title={card.title}
          >
            <div className="quality-card-topline">
              <span className="quality-variable">{card.variable}</span>
              <span className="quality-score">{card.value}</span>
            </div>
            <span className="quality-problem-type">{card.problem}</span>
            <div
              className="quality-meter"
              role="progressbar"
              aria-label={`${card.problem} en ${card.variable}`}
              aria-valuemin="0"
              aria-valuemax="100"
              aria-valuenow={card.score}
            >
              <span style={{ width: `${card.score}%` }} />
            </div>
            <div className="quality-card-footer">
              <span>Calidad del indicador: {card.score}%</span>
              {card.trend && (
                <span className={`quality-trend is-${card.trend.status}`}>
                  {card.trend.label} · {formatDelta(card.trend.delta)}
                </span>
              )}
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}
