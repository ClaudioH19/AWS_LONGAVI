const UNIT_RULES = [
  { pattern: /temp|temperature|temperatura|ch2/, unit: '°C' },
  { pattern: /hum|humidity|humedad|rh|hr|^$/, unit: '%' },
  { pattern: /rad|solar|radiaci|irradiance|radiation|ch4/, unit: 'W/m²' },
  { pattern: /press|presion|pressure/, unit: 'hPa' },
  { pattern: /dir|wind direction|direccion|ch1/, unit: '°' },
  { pattern: /vel|wind speed|viento|anemo|ch0/, unit: 'm/s' },
  { pattern: /rain|lluvia|precip|ch3/, unit: 'mm' },
];

const DISPLAY_RULES = [
  { pattern: /temp|temperature|temperatura|ch2/, label: 'Temperatura' },
  { pattern: /hum|humidity|humedad|rh|hr|^$/, label: 'Humedad' },
  { pattern: /rain|lluvia|precip|ch3/, label: 'Precipitación' },
  { pattern: /rad|solar|radiaci|irradiance|radiation|ch4/, label: 'Radiación solar' },
  { pattern: /vel|wind speed|viento|anemo|ch0/, label: 'Velocidad' },
  { pattern: /dir|wind direction|direccion|ch1/, label: 'Dirección' },
];

export const WEATHER_NUMERIC_EXCLUDED_KEYS = new Set([
  'id',
  'received_at',
  'Timestamp',
  'DeviceID',
  'DeviceType',
  'DeviceVersion',
]);

export const WEATHER_PREFERRED_KEYS = [
  'Temp',
  'Hum',
  'Vel',
  'Dir',
  'Precip',
  'Rad',
  'Temperature',
  'Humidity',
  'Wind Speed',
  'Wind Direction',
  'Rain 24h',
  'Solar Radiation',
  '',
  'ch2',
  'ch0',
  'ch1',
  'ch3',
  'ch4',
];

export const WEATHER_FIXED_KEYS = ['Temp', 'Hum', 'Vel', 'Dir', 'Precip', 'Rad'];

export function getUnitForKey(key = '') {
  const normalized = String(key).toLowerCase().trim();
  const match = UNIT_RULES.find((rule) => rule.pattern.test(normalized));
  return match ? match.unit : '';
}

export function getVariableDisplayName(key = '') {
  const normalized = String(key).toLowerCase().trim();
  const match = DISPLAY_RULES.find((rule) => rule.pattern.test(normalized));
  return match ? match.label : key;
}

export function scaleWeatherValue(key = '', value) {
  if (value === null || value === undefined || value === '') return null;
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return null;

  if (getUnitForKey(key)) {
    return Number((numericValue / 10).toFixed(2));
  }

  return numericValue;
}

export function formatWeatherValue(key = '', value, fallback = '--') {
  const scaledValue = scaleWeatherValue(key, value);
  if (scaledValue === null) return fallback;

  const unit = getUnitForKey(key);
  const formatted = scaledValue.toLocaleString('es-CL', {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });

  return unit ? `${formatted} ${unit}` : formatted;
}
