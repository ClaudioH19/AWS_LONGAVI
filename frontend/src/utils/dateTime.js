const DATE_TIME_REGEX = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/;

const WEEKDAY_LABELS_ES = ['Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb'];

function getDatePart(parts, type) {
  return parts.find((part) => part.type === type)?.value || '00';
}

export function getTodayInChileDateInput() {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Santiago',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  const parts = formatter.formatToParts(new Date());
  const year = getDatePart(parts, 'year');
  const month = getDatePart(parts, 'month');
  const day = getDatePart(parts, 'day');
  return `${year}-${month}-${day}`;
}

export function formatDateTime(value, fallback = '-') {
  if (!value) return fallback;
  const text = String(value).trim();
  const match = text.match(DATE_TIME_REGEX);
  if (!match) return text.replace('T', ' ');
  const [, year, month, day, hour, minute, second = '00'] = match;
  return `${day}/${month}/${year} ${hour}:${minute}:${second}`;
}

export function formatDateTimeShort(value, fallback = '') {
  if (!value) return fallback;
  const text = String(value).trim();
  const match = text.match(DATE_TIME_REGEX);
  if (!match) return text.replace('T', ' ');
  const [, , month, day, hour, minute] = match;
  return `${day}/${month} ${hour}:${minute}`;
}

export function parseDateTimeParts(value) {
  if (!value) return null;
  const text = String(value).trim();
  const match = text.match(DATE_TIME_REGEX);
  if (!match) return null;

  const [, year, month, day, hour, minute, second = '00'] = match;
  return {
    year: Number(year),
    month: Number(month),
    day: Number(day),
    hour: Number(hour),
    minute: Number(minute),
    second: Number(second),
  };
}

export function parseDateTimeAsLocal(value) {
  const parts = parseDateTimeParts(value);
  if (!parts) return null;

  return new Date(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second,
  );
}

// NUEVO: día de la semana calculado SIN pasar por new Date(rawString),
// para que nunca se desalinee con formatDateTime/formatDateTimeShort.
export function getDayOfWeekIndex(value) {
  const date = parseDateTimeAsLocal(value);
  if (!date) return null;
  return date.getDay(); // 0 = Domingo ... 6 = Sábado
}

export function getDayOfWeekLabel(value, fallback = '') {
  const index = getDayOfWeekIndex(value);
  if (index === null) return fallback;
  return WEEKDAY_LABELS_ES[index];
}