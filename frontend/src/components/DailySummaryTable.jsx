import { useMemo } from 'react';
import StatusState from './StatusState';
import { parseDateTimeParts } from '../utils/dateTime';
import { getUnitForKey, scaleWeatherValue } from '../utils/weatherVariables';

const WEEKDAY_LABELS = ['Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb'];

function formatValue(value, unit) {
  if (value === null || value === undefined) return '--';
  const formatted = value.toLocaleString('es-CL', {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });
  return unit ? `${formatted} ${unit}` : formatted;
}

function average(values) {
  if (!values.length) return null;
  return values.reduce((total, value) => total + value, 0) / values.length;
}

function buildDailySummaries(rows) {
  const days = new Map();

  rows.forEach((row) => {
    const parts = parseDateTimeParts(row.received_at);
    if (!parts) return;

    const key = `${parts.year}-${String(parts.month).padStart(2, '0')}-${String(parts.day).padStart(2, '0')}`;
    if (!days.has(key)) {
      days.set(key, {
        key,
        date: new Date(Date.UTC(parts.year, parts.month - 1, parts.day)),
        temperature: [],
        humidity: [],
        precipitation: [],
        wind: [],
        radiation: [],
      });
    }

    const day = days.get(key);
    const metrics = [
      ['temperature', 'Temp'],
      ['humidity', 'Hum'],
      ['precipitation', 'Precip'],
      ['wind', 'Vel'],
      ['radiation', 'Rad'],
    ];
    metrics.forEach(([property, weatherKey]) => {
      const value = scaleWeatherValue(weatherKey, row[weatherKey]);
      if (value !== null) day[property].push(value);
    });
  });

  return [...days.values()]
    .sort((left, right) => left.key.localeCompare(right.key))
    .map((day) => {
      const rain = day.precipitation.reduce((total, value) => total + value, 0);
      return {
        ...day,
        maxTemperature: day.temperature.length ? Math.max(...day.temperature) : null,
        minTemperature: day.temperature.length ? Math.min(...day.temperature) : null,
        humidity: average(day.humidity),
        rain,
        wind: average(day.wind),
        radiation: average(day.radiation),
      };
    });
}

export default function DailySummaryTable({ rows }) {
  const dailySummaries = useMemo(() => buildDailySummaries(rows), [rows]);

  if (!dailySummaries.length) {
    return (
      <StatusState
        title="Sin resumen diario"
        message="Aún no hay lecturas suficientes para calcular el detalle por día."
      />
    );
  }

  return (
    <section className="daily-summary" aria-labelledby="daily-summary-title">
      <div className="daily-summary-heading">
        <span className="panel-kicker">Detalle diario</span>
        <h3 id="daily-summary-title">Resumen meteorológico por día</h3>
      </div>
      <div className="daily-summary-scroll">
        <table className="daily-summary-table">
          <thead>
            <tr>
              <th>Fecha</th>
              <th>Radiación solar</th>
              <th>Temp. máx.</th>
              <th>Temp. mín.</th>
              <th>Humedad</th>
              <th>Lluvia acum.</th>
              <th>Viento</th>
            </tr>
          </thead>
          <tbody>
            {dailySummaries.map((day) => (
              <tr key={day.key}>
                <td className="daily-date">
                  {WEEKDAY_LABELS[day.date.getUTCDay()]} {String(day.date.getUTCDate()).padStart(2, '0')}/{String(day.date.getUTCMonth() + 1).padStart(2, '0')}
                </td>
                <td><span className="daily-metric is-radiation">{formatValue(day.radiation, getUnitForKey('Rad'))}</span></td>
                <td><span className="daily-metric is-temp-max">{formatValue(day.maxTemperature, getUnitForKey('Temp'))}</span></td>
                <td><span className="daily-metric is-temp-min">{formatValue(day.minTemperature, getUnitForKey('Temp'))}</span></td>
                <td><span className="daily-metric is-humidity">{formatValue(day.humidity, getUnitForKey('Hum'))}</span></td>
                <td><span className="daily-metric is-rain">{formatValue(day.rain, getUnitForKey('Precip'))}</span></td>
                <td><span className="daily-metric is-wind">{formatValue(day.wind, getUnitForKey('Vel'))}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
