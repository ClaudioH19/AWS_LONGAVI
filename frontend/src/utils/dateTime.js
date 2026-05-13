const DATE_TIME_REGEX = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/;

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
