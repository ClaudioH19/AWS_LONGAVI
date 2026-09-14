import { useMemo } from 'react';
import { AGRONOMIC_THRESHOLDS } from '../config/agronomicThresholds';
import { buildChartOptions } from '../config/weatherChartOptions';
import { WEATHER_VISUAL_THEME } from '../config/weatherVisualTheme';
import { metricSeries } from '../utils/weatherChartSeries';
import WeatherChartCard from './WeatherChartCard';

export default function FixedWeatherCharts({ dailyStatistics }) {
  const labels = useMemo(() => dailyStatistics.map(({ label }) => label), [dailyStatistics]);
  const temperature = WEATHER_VISUAL_THEME.temperature;
  const humidity = WEATHER_VISUAL_THEME.humidity;
  const rain = WEATHER_VISUAL_THEME.rain;
  const radiation = WEATHER_VISUAL_THEME.radiation;
  const wind = WEATHER_VISUAL_THEME.wind;

  const rainData = useMemo(() => ({
    labels,
    datasets: [{
      label: 'Lluvia acumulada (mm)',
      data: metricSeries(dailyStatistics, 'Precip', 'sum'),
      backgroundColor: rain.soft,
      borderColor: rain.secondary,
      borderWidth: 1.5,
      borderRadius: 10,
      borderSkipped: false,
      maxBarThickness: 72,
    }],
  }), [dailyStatistics, labels, rain]);

  const temperatureHumidityData = useMemo(() => ({
    labels,
    datasets: [
      {
        label: 'Máxima (°C)',
        data: metricSeries(dailyStatistics, 'Temp', 'max'),
        borderColor: 'rgba(146, 64, 14, 0.2)',
        backgroundColor: 'rgba(254, 243, 199, 0.44)',
        pointRadius: 0,
        borderWidth: 1,
        tension: 0.32,
        hideFromLegend: true,
        hideFromTooltip: true,
        order: 4,
      },
      {
        label: 'Rango térmico',
        data: metricSeries(dailyStatistics, 'Temp', 'min'),
        borderColor: 'rgba(154, 52, 18, 0.22)',
        backgroundColor: 'rgba(254, 215, 170, 0.34)',
        pointRadius: 0,
        borderWidth: 1,
        fill: '-1',
        tension: 0.32,
        hideFromLegend: true,
        hideFromTooltip: true,
        order: 3,
      },
      {
        label: 'Temperatura media (°C)',
        data: metricSeries(dailyStatistics, 'Temp', 'average'),
        borderColor: temperature.ink,
        backgroundColor: temperature.ink,
        pointBackgroundColor: '#ffffff',
        pointBorderColor: temperature.ink,
        pointBorderWidth: 2,
        pointRadius: 3,
        tension: 0.32,
        yAxisID: 'y',
        order: 1,
      },
      {
        label: 'Humedad media (%)',
        data: metricSeries(dailyStatistics, 'Hum', 'average'),
        borderColor: humidity.ink,
        backgroundColor: humidity.ink,
        borderDash: [6, 4],
        pointBackgroundColor: '#ffffff',
        pointBorderColor: humidity.ink,
        pointBorderWidth: 2,
        pointRadius: 3,
        tension: 0.32,
        yAxisID: 'y1',
        order: 2,
      },
    ],
  }), [dailyStatistics, humidity, labels, temperature]);

  const radiationData = useMemo(() => ({
    labels,
    datasets: [{
      label: 'Radiación media (W/m²)',
      data: metricSeries(dailyStatistics, 'Rad', 'average'),
      borderColor: radiation.ink,
      backgroundColor: 'rgba(202, 138, 4, 0.16)',
      fill: 'origin',
      pointBackgroundColor: '#ffffff',
      pointBorderColor: radiation.ink,
      pointBorderWidth: 2,
      pointRadius: 3,
      tension: 0.36,
    }],
  }), [dailyStatistics, labels, radiation]);

  const windData = useMemo(() => {
    const safeThreshold = AGRONOMIC_THRESHOLDS.Vel.safe;
    const dangerThreshold = AGRONOMIC_THRESHOLDS.Vel.danger;
    return {
      labels,
      datasets: [
        {
          label: 'Viento medio (m/s)',
          data: metricSeries(dailyStatistics, 'Vel', 'average'),
          borderColor: wind.ink,
          backgroundColor: wind.ink,
          pointBackgroundColor: '#ffffff',
          pointBorderColor: wind.ink,
          pointBorderWidth: 2,
          pointRadius: 3,
          tension: 0.32,
        },
        {
          label: 'Máximo diario (m/s)',
          data: metricSeries(dailyStatistics, 'Vel', 'max'),
          borderColor: wind.secondary,
          backgroundColor: wind.secondary,
          borderDash: [6, 4],
          pointRadius: 2,
          tension: 0.28,
        },
        {
          label: `Referencia ${safeThreshold} m/s`,
          data: labels.map(() => safeThreshold),
          borderColor: 'rgba(22, 101, 52, 0.28)',
          borderDash: [3, 5],
          borderWidth: 1,
          pointRadius: 0,
          hideFromLegend: true,
          hideFromTooltip: true,
        },
        {
          label: `Referencia ${dangerThreshold} m/s`,
          data: labels.map(() => dangerThreshold),
          borderColor: 'rgba(180, 35, 24, 0.34)',
          borderDash: [3, 5],
          borderWidth: 1,
          pointRadius: 0,
          hideFromLegend: true,
          hideFromTooltip: true,
        },
      ],
    };
  }, [dailyStatistics, labels, wind]);

  const precipitationOptions = useMemo(
    () => buildChartOptions({ leftAxisTitle: 'Milímetros', leftBeginAtZero: true }),
    [],
  );
  const temperatureHumidityOptions = useMemo(
    () => buildChartOptions({
      leftAxisTitle: 'Temperatura (°C)',
      rightAxisTitle: 'Humedad (%)',
      leftBeginAtZero: false,
      rightBeginAtZero: true,
    }),
    [],
  );
  const radiationOptions = useMemo(
    () => buildChartOptions({ leftAxisTitle: 'W/m²', leftBeginAtZero: true }),
    [],
  );
  const windOptions = useMemo(
    () => buildChartOptions({ leftAxisTitle: 'm/s', leftBeginAtZero: true }),
    [],
  );

  return (
    <div className="fixed-chart-grid">
      <WeatherChartCard
        id="rain-chart-title"
        eyebrow="Acumulación"
        title="Lluvia de los últimos 7 días"
        description="Suma diaria"
        tone="rain"
        type="bar"
        data={rainData}
        options={precipitationOptions}
      />
      <WeatherChartCard
        id="temperature-humidity-chart-title"
        eyebrow="Confort ambiental"
        title="Temperatura y humedad"
        description="Media y rango diario"
        tone="temperature"
        data={temperatureHumidityData}
        options={temperatureHumidityOptions}
      />
      <WeatherChartCard
        id="radiation-chart-title"
        eyebrow="Energía disponible"
        title="Radiación solar"
        description="Promedio diario"
        tone="radiation"
        data={radiationData}
        options={radiationOptions}
      />
      <WeatherChartCard
        id="wind-chart-title"
        eyebrow="Ventilación"
        title="Velocidad del viento"
        description="Media y máximo diario"
        tone="wind"
        data={windData}
        options={windOptions}
      />
    </div>
  );
}
