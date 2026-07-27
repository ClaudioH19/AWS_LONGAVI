import { parseDateTimeParts } from './dateTime';
import {
  WEATHER_FIXED_KEYS,
  getVariableDisplayName,
  scaleWeatherValue,
} from './weatherVariables';

const PLAUSIBLE_RANGES = {
  Temp: [-30, 60],
  Hum: [0, 100],
  Vel: [0, 80],
  Dir: [0, 360],
  Precip: [0, 500],
  Rad: [0, 1600],
};

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
}

function clampScore(value) {
  return Math.max(0, Math.min(100, value));
}

function toTimestamp(value) {
  const parts = parseDateTimeParts(value);
  if (!parts) return null;
  return Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second,
  );
}

function toDateKey(value) {
  const parts = parseDateTimeParts(value);
  if (!parts) return null;
  return `${parts.year}-${String(parts.month).padStart(2, '0')}-${String(parts.day).padStart(2, '0')}`;
}

function getExpectedInterval(rows) {
  const timestamps = rows
    .map((row) => toTimestamp(row.received_at))
    .filter(Number.isFinite)
    .sort((left, right) => left - right);
  const intervals = timestamps
    .slice(1)
    .map((timestamp, index) => timestamp - timestamps[index])
    .filter((interval) => interval > 0);
  return median(intervals) || 60 * 1000;
}

function getPlausibilityScore(key, values) {
  if (!values.length) return 0;
  const [minimum, maximum] = PLAUSIBLE_RANGES[key] || [-Infinity, Infinity];
  const plausibleCount = values.filter((value) => value >= minimum && value <= maximum).length;
  return (plausibleCount / values.length) * 100;
}

function getContinuityScore(timestamps, expectedInterval) {
  if (timestamps.length < 2) return timestamps.length ? 100 : 0;
  const ordered = [...timestamps].sort((left, right) => left - right);
  const acceptableGap = expectedInterval * 2.5;
  const continuousIntervals = ordered
    .slice(1)
    .filter((timestamp, index) => timestamp - ordered[index] <= acceptableGap)
    .length;
  return (continuousIntervals / (ordered.length - 1)) * 100;
}

function getFreshnessScore(validTimestamps, referenceTimestamp, expectedInterval) {
  if (!validTimestamps.length || !Number.isFinite(referenceTimestamp)) return 0;
  const lastValidTimestamp = Math.max(...validTimestamps);
  const delayedIntervals = Math.max(0, referenceTimestamp - lastValidTimestamp) / expectedInterval;
  if (delayedIntervals <= 1.5) return 100;
  return clampScore(100 - ((delayedIntervals - 1.5) / 4.5) * 100);
}

function calculateScore(rows, key, expectedInterval) {
  if (!rows.length) return null;

  const referenceTimestamp = Math.max(
    ...rows.map((row) => toTimestamp(row.received_at)).filter(Number.isFinite),
  );
  const validSamples = rows
    .map((row) => ({
      value: scaleWeatherValue(key, row[key]),
      timestamp: toTimestamp(row.received_at),
    }))
    .filter(({ value, timestamp }) => value !== null && Number.isFinite(timestamp));
  const values = validSamples.map(({ value }) => value);
  const timestamps = validSamples.map(({ timestamp }) => timestamp);
  const completeness = (validSamples.length / rows.length) * 100;
  const plausibility = getPlausibilityScore(key, values);
  const continuity = getContinuityScore(timestamps, expectedInterval);
  const freshness = getFreshnessScore(timestamps, referenceTimestamp, expectedInterval);
  const score = (
    completeness * 0.4
    + plausibility * 0.3
    + continuity * 0.2
    + freshness * 0.1
  );

  return {
    score: Math.round(clampScore(score)),
    completeness: Math.round(completeness),
    plausibility: Math.round(plausibility),
    continuity: Math.round(continuity),
    freshness: Math.round(freshness),
  };
}

function getQualityLevel(score) {
  if (score >= 90) return { status: 'excellent', label: 'Excelente' };
  if (score >= 75) return { status: 'good', label: 'Buena' };
  if (score >= 55) return { status: 'warning', label: 'En observación' };
  return { status: 'poor', label: 'Baja' };
}

function compareMetric(currentValue, baselineValue) {
  if (!Number.isFinite(currentValue) || !Number.isFinite(baselineValue)) {
    return { status: 'unknown', label: 'Sin referencia', delta: null };
  }

  const delta = Math.round(currentValue - baselineValue);
  if (delta <= -5) return { status: 'down', label: 'Deteriorándose', delta };
  if (delta >= 5) return { status: 'up', label: 'Mejorando', delta };
  return { status: 'stable', label: 'Estable', delta };
}

const PROBLEM_DEFINITIONS = [
  {
    key: 'completeness',
    label: 'Datos faltantes',
    threshold: 98,
    criticalThreshold: 90,
    formatValue: (score) => `${Math.max(0, 100 - score)}% faltante`,
  },
  {
    key: 'plausibility',
    label: 'Valores fuera de rango',
    threshold: 99,
    criticalThreshold: 95,
    formatValue: (score) => `${Math.max(0, 100 - score)}% fuera de rango`,
  },
  {
    key: 'continuity',
    label: 'Cortes de continuidad',
    threshold: 95,
    criticalThreshold: 80,
    formatValue: (score) => `${Math.max(0, 100 - score)}% de cortes`,
  },
  {
    key: 'freshness',
    label: 'Lectura desactualizada',
    threshold: 80,
    criticalThreshold: 40,
    formatValue: (score) => `${score}% de frescura`,
  },
];

function buildProblems(currentMetrics, baselineMetrics) {
  return PROBLEM_DEFINITIONS.flatMap((definition) => {
    const score = currentMetrics[definition.key];
    const trend = compareMetric(score, baselineMetrics?.[definition.key]);
    const hasProblem = score < definition.threshold || trend.status === 'down';
    if (!hasProblem) return [];

    return [{
      key: definition.key,
      label: definition.label,
      value: definition.formatValue(score),
      score,
      severity: score < definition.criticalThreshold ? 'poor' : 'warning',
      trend,
    }];
  });
}

export function buildVariableQualitySummaries(rows, days) {
  const expectedInterval = getExpectedInterval(rows);
  const recentDayKeys = new Set(days.slice(-7).map(({ key }) => key));
  const baselineDayKeys = new Set(days.slice(0, -7).map(({ key }) => key));
  const recentRows = rows.filter((row) => recentDayKeys.has(toDateKey(row.received_at)));
  const baselineRows = rows.filter((row) => baselineDayKeys.has(toDateKey(row.received_at)));

  return WEATHER_FIXED_KEYS.map((key) => {
    const metrics = calculateScore(recentRows, key, expectedInterval) || {
      score: 0,
      completeness: 0,
      plausibility: 0,
      continuity: 0,
      freshness: 0,
    };
    const baselineMetrics = calculateScore(baselineRows, key, expectedInterval);
    return {
      key,
      name: getVariableDisplayName(key),
      ...metrics,
      level: getQualityLevel(metrics.score),
      problems: buildProblems(metrics, baselineMetrics),
    };
  });
}
