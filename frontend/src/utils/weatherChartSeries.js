import { getTodayInChileDateInput, parseDateTimeParts } from './dateTime.js';
import { WEATHER_FIXED_KEYS, scaleWeatherValue } from './weatherVariables.js';

const WEEKDAY_LABELS = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom'];

function parseDateParts(value) {
  const match = String(value || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  return {
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
  };
}

function toDateInputFromUTC(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

export function getTrailingDaysBounds(totalDays = 7, dateInput = getTodayInChileDateInput()) {
  const today = parseDateParts(dateInput);
  if (!today) return { desde: '', hasta: '' };

  const todayDate = new Date(Date.UTC(today.year, today.month - 1, today.day));
  const firstDay = new Date(todayDate);
  firstDay.setUTCDate(todayDate.getUTCDate() - (totalDays - 1));
  return {
    desde: toDateInputFromUTC(firstDay),
    hasta: toDateInputFromUTC(todayDate),
  };
}

export function buildTrailingDays(totalDays = 7, dateInput = getTodayInChileDateInput()) {
  const start = parseDateParts(getTrailingDaysBounds(totalDays, dateInput).desde);
  if (!start) return [];

  const firstDay = new Date(Date.UTC(start.year, start.month - 1, start.day));
  return Array.from({ length: totalDays }, (_, index) => {
    const date = new Date(firstDay);
    date.setUTCDate(firstDay.getUTCDate() + index);
    const weekdayIndex = (date.getUTCDay() + 6) % 7;
    return {
      key: toDateInputFromUTC(date),
      label: `${WEEKDAY_LABELS[weekdayIndex]} ${String(date.getUTCDate()).padStart(2, '0')}/${String(date.getUTCMonth() + 1).padStart(2, '0')}`,
    };
  });
}

function dateInputFromReading(value) {
  const parts = parseDateTimeParts(value);
  if (!parts) return null;
  return toDateInputFromUTC(new Date(Date.UTC(parts.year, parts.month - 1, parts.day)));
}

function summarize(values) {
  if (!values.length) {
    return { average: null, min: null, max: null, sum: null };
  }
  const sum = values.reduce((total, value) => total + value, 0);
  return {
    average: Number((sum / values.length).toFixed(2)),
    min: Number(Math.min(...values).toFixed(2)),
    max: Number(Math.max(...values).toFixed(2)),
    sum: Number(sum.toFixed(2)),
  };
}

export function buildDailyWeatherStatistics(rows, days) {
  const buckets = new Map(days.map(({ key }) => [
    key,
    Object.fromEntries(WEATHER_FIXED_KEYS.map((weatherKey) => [weatherKey, []])),
  ]));

  rows.forEach((row) => {
    const day = dateInputFromReading(row.received_at);
    const bucket = buckets.get(day);
    if (!bucket) return;

    WEATHER_FIXED_KEYS.forEach((weatherKey) => {
      const value = scaleWeatherValue(weatherKey, row[weatherKey]);
      if (value !== null) bucket[weatherKey].push(value);
    });
  });

  return days.map(({ key, label }) => ({
    key,
    label,
    metrics: Object.fromEntries(
      WEATHER_FIXED_KEYS.map((weatherKey) => [weatherKey, summarize(buckets.get(key)[weatherKey])]),
    ),
  }));
}

export function metricSeries(dailyStatistics, weatherKey, aggregation = 'average') {
  return dailyStatistics.map((day) => day.metrics[weatherKey]?.[aggregation] ?? null);
}
