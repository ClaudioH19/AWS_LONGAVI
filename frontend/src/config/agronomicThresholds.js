export const AGRONOMIC_THRESHOLDS = {
  temperature: { cold: 5, hot: 26 },
  humidity: { low: 40, high: 70 },
  radiation: { high: 500 },
};

export function getAgronomicTone(weatherKey, value) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return 'neutral';
  }

  if (weatherKey === 'Temp') {
    if (value < AGRONOMIC_THRESHOLDS.temperature.cold) return 'temperature-cold';
    if (value > AGRONOMIC_THRESHOLDS.temperature.hot) return 'temperature-hot';
    return 'temperature-optimal';
  }

  if (weatherKey === 'Hum') {
    if (value < AGRONOMIC_THRESHOLDS.humidity.low) return 'humidity-low';
    if (value > AGRONOMIC_THRESHOLDS.humidity.high) return 'humidity-high';
    return 'humidity-optimal';
  }

  if (weatherKey === 'Precip') {
    return value > 0 ? 'precipitation-present' : 'precipitation-none';
  }

  if (weatherKey === 'Rad') {
    return value > AGRONOMIC_THRESHOLDS.radiation.high ? 'radiation-high' : 'radiation-low';
  }

  return 'neutral';
}
