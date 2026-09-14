import { useMemo, useState } from 'react';
import { Chart } from 'react-chartjs-2';
import { buildChartOptions } from '../config/weatherChartOptions';
import { VARIABLE_THEME } from '../config/weatherVisualTheme';
import { metricSeries } from '../utils/weatherChartSeries';
import {
  WEATHER_FIXED_KEYS,
  getUnitForKey,
  getVariableDisplayName,
} from '../utils/weatherVariables';

function seriesLabel(key) {
  const unit = getUnitForKey(key);
  const name = getVariableDisplayName(key);
  return unit ? `${name} (${unit})` : name;
}

export default function GeneralComparisonChart({ dailyStatistics }) {
  const [lineKey, setLineKey] = useState('Temp');
  const [barKey, setBarKey] = useState('Hum');
  const labels = useMemo(() => dailyStatistics.map(({ label }) => label), [dailyStatistics]);

  const chartData = useMemo(() => ({
    labels,
    datasets: [
      {
        type: 'bar',
        label: seriesLabel(barKey),
        data: metricSeries(
          dailyStatistics,
          barKey,
          barKey === 'Precip' ? 'sum' : 'average',
        ),
        backgroundColor: VARIABLE_THEME[barKey].soft,
        borderColor: VARIABLE_THEME[barKey].secondary,
        borderRadius: 8,
        borderSkipped: false,
        borderWidth: 1,
        maxBarThickness: 64,
        yAxisID: 'y1',
        order: 3,
      },
      {
        type: 'line',
        label: seriesLabel(lineKey),
        data: metricSeries(dailyStatistics, lineKey),
        borderColor: VARIABLE_THEME[lineKey].ink,
        backgroundColor: VARIABLE_THEME[lineKey].ink,
        pointBackgroundColor: '#ffffff',
        pointBorderColor: VARIABLE_THEME[lineKey].ink,
        pointBorderWidth: 2,
        pointRadius: 3,
        tension: 0.32,
        yAxisID: 'y',
        order: 1,
      },
    ],
  }), [barKey, dailyStatistics, labels, lineKey]);

  const options = useMemo(() => buildChartOptions({
    leftAxisTitle: seriesLabel(lineKey),
    rightAxisTitle: seriesLabel(barKey),
    leftBeginAtZero: lineKey !== 'Temp',
    rightBeginAtZero: barKey !== 'Temp',
  }), [barKey, lineKey]);

  return (
    <section className="general-chart-view" aria-labelledby="general-chart-title">
      <div className="panel-toolbar general-chart-toolbar">
        <label className="variable-inline-selector">
          Línea
          <select value={lineKey} onChange={(event) => setLineKey(event.target.value)}>
            {WEATHER_FIXED_KEYS.map((key) => (
              <option key={`line-${key}`} value={key}>{getVariableDisplayName(key)}</option>
            ))}
          </select>
        </label>
        <label className="variable-inline-selector">
          Barras
          <select value={barKey} onChange={(event) => setBarKey(event.target.value)}>
            {WEATHER_FIXED_KEYS.map((key) => (
              <option key={`bar-${key}`} value={key}>{getVariableDisplayName(key)}</option>
            ))}
          </select>
        </label>
      </div>
      <div className="trend-chart">
        <div>
          <span className="chart-kicker">Explorador móvil</span>
          <h3 id="general-chart-title">
            {getVariableDisplayName(lineKey)} y {getVariableDisplayName(barKey)}
          </h3>
        </div>
        <div className="chart-canvas-shell">
          <Chart type="bar" data={chartData} options={options} />
        </div>
      </div>
    </section>
  );
}
