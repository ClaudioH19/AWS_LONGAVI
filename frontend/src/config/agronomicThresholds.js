export const AGRONOMIC_THRESHOLDS = {
  Temp: { cold: 5, hot: 26 },
  Hum: { safe: [[40, 70]], normal: [[30, 40], [70, 80]] },
  Precip: { moderate: 5 },
  Rad: { low: 200, high: 800 },
  Vel: { safe: 5, danger: 8 },
  Dir: {},
};

function isWithinRanges(value, ranges) {
  return ranges.some(([min, max]) => value >= min && value <= max);
}

export function getAgronomicTone(weatherKey, value) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return 'neutral';
  }

  const thresholds = AGRONOMIC_THRESHOLDS[weatherKey];
  if (!thresholds) return 'normal';

  if (weatherKey === 'Temp') {
    if (value < thresholds.cold) return 'temperature-cold';
    if (value > thresholds.hot) return 'temperature-hot';
    return 'temperature-normal';
  }

  if (weatherKey === 'Precip') {
    if (value === 0) return 'precipitation-none';
    if (value <= thresholds.moderate) return 'precipitation-present';
    return 'precipitation-heavy';
  }

  if (weatherKey === 'Rad') {
    if (value < thresholds.low) return 'radiation-low';
    if (value <= thresholds.high) return 'radiation-normal';
    return 'radiation-high';
  }

  if (weatherKey === 'Vel') {
    if (value <= thresholds.safe) return 'wind-safe';
    if (value <= thresholds.danger) return 'wind-normal';
    return 'wind-danger';
  }

  if (weatherKey === 'Dir') return 'normal';

  if (isWithinRanges(value, thresholds.safe)) return 'safe';
  if (isWithinRanges(value, thresholds.normal)) return 'normal';
  return 'danger';
}
