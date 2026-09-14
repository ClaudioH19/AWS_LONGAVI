export const WEATHER_VISUAL_THEME = {
  temperature: {
    ink: '#92400e',
    secondary: '#9a3412',
    soft: '#fef3c7',
    softSecondary: '#ffedd5',
  },
  humidity: {
    ink: '#1e3a8a',
    secondary: '#2563eb',
    soft: '#dbeafe',
  },
  rain: {
    ink: '#075985',
    secondary: '#0284c7',
    soft: '#e0f2fe',
  },
  radiation: {
    ink: '#a16207',
    secondary: '#ca8a04',
    soft: '#fef3c7',
  },
  wind: {
    ink: '#166534',
    secondary: '#16a34a',
    soft: '#dcfce7',
  },
  direction: {
    ink: '#475569',
    secondary: '#64748b',
    soft: '#f1f5f9',
  },
};

export const VARIABLE_THEME = {
  Temp: WEATHER_VISUAL_THEME.temperature,
  Hum: WEATHER_VISUAL_THEME.humidity,
  Precip: WEATHER_VISUAL_THEME.rain,
  Rad: WEATHER_VISUAL_THEME.radiation,
  Vel: WEATHER_VISUAL_THEME.wind,
  Dir: WEATHER_VISUAL_THEME.direction,
};

export const CHART_NEUTRALS = {
  grid: 'rgba(100, 116, 139, 0.16)',
  text: '#334155',
  tooltip: 'rgba(15, 23, 42, 0.94)',
};
