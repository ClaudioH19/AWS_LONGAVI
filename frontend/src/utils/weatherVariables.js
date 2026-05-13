const UNIT_RULES = [
  { pattern: /temp|temperature|temperatura|ch2/, unit: 'C' },
  { pattern: /hum|humidity|humedad|rh|hr|^$/, unit: '%' },
  { pattern: /rad|solar|radiaci|irradiance|radiation|ch4/, unit: 'W/m2' },
  { pattern: /press|presion|pressure/, unit: 'hPa' },
  { pattern: /dir|wind direction|direccion|ch1/, unit: 'deg' },
  { pattern: /vel|wind speed|viento|anemo|ch0/, unit: 'm/s' },
  { pattern: /rain|lluvia|precip|ch3/, unit: 'mm' },
];

const DISPLAY_RULES = [
  { pattern: /temp|temperature|temperatura|ch2/, label: 'Temp' },
  { pattern: /hum|humidity|humedad|rh|hr|^$/, label: 'Hum' },
  { pattern: /rain|lluvia|precip|ch3/, label: 'Precip' },
  { pattern: /rad|solar|radiaci|irradiance|radiation|ch4/, label: 'Rad' },
  { pattern: /vel|wind speed|viento|anemo|ch0/, label: 'Vel' },
  { pattern: /dir|wind direction|direccion|ch1/, label: 'Dir' },
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
