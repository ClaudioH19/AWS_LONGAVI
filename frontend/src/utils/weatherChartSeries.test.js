import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildDailyWeatherStatistics,
  buildTrailingDays,
  getTrailingDaysBounds,
  metricSeries,
} from './weatherChartSeries.js';

test('construye exactamente los siete días terminados en la fecha de Chile', () => {
  assert.deepEqual(getTrailingDaysBounds(7, '2026-09-14'), {
    desde: '2026-09-08',
    hasta: '2026-09-14',
  });
  const days = buildTrailingDays(7, '2026-09-14');
  assert.equal(days.length, 7);
  assert.equal(days[0].key, '2026-09-08');
  assert.equal(days[6].key, '2026-09-14');
});

test('todos los gráficos comparten promedios, extremos y acumulados diarios', () => {
  const days = buildTrailingDays(7, '2026-09-14');
  const rows = [
    { received_at: '2026-09-13 10:00:00', Temp: 200, Hum: 500, Precip: 10, Rad: 6000, Vel: 20 },
    { received_at: '2026-09-13 14:00:00', Temp: 220, Hum: 700, Precip: 20, Rad: 8000, Vel: 40 },
    { received_at: '2026-09-01 14:00:00', Temp: 999, Hum: 999, Precip: 999, Rad: 999, Vel: 999 },
  ];
  const statistics = buildDailyWeatherStatistics(rows, days);

  assert.equal(statistics[5].metrics.Temp.average, 21);
  assert.equal(statistics[5].metrics.Temp.min, 20);
  assert.equal(statistics[5].metrics.Temp.max, 22);
  assert.equal(statistics[5].metrics.Hum.average, 60);
  assert.equal(statistics[5].metrics.Precip.sum, 3);
  assert.equal(statistics[5].metrics.Rad.average, 700);
  assert.equal(statistics[5].metrics.Vel.max, 4);
  assert.deepEqual(metricSeries(statistics, 'Temp', 'average'), [null, null, null, null, null, 21, null]);
});
