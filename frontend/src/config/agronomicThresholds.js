export const AGRONOMIC_THRESHOLDS = {
  Temp: { safe: [[10, 26]], normal: [[5, 10], [26, 32]] },
  Hum: { safe: [[40, 70]], normal: [[30, 40], [70, 80]] },
  Precip: { safe: [[0.1, 5]], normal: [[0, 0]] },
  Rad: { safe: [[200, 600]], normal: [[0, 200], [600, 800]] },
  Vel: { safe: [[0, 5]], normal: [[5, 8]] },
  Dir: { safe: [], normal: [[0, 360]] },
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
  if (isWithinRanges(value, thresholds.safe)) return 'safe';
  if (isWithinRanges(value, thresholds.normal)) return 'normal';
  return 'danger';
}
