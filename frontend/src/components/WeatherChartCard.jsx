import { Chart } from 'react-chartjs-2';

export default function WeatherChartCard({
  id,
  eyebrow,
  title,
  description,
  tone,
  type = 'line',
  data,
  options,
}) {
  return (
    <section className={`weather-chart-card is-${tone}`} aria-labelledby={id}>
      <header className="weather-chart-header">
        <div>
          <span className="chart-kicker">{eyebrow}</span>
          <h3 id={id}>{title}</h3>
        </div>
        <p>{description}</p>
      </header>
      <div className="weather-chart-canvas">
        <Chart type={type} data={data} options={options} />
      </div>
    </section>
  );
}
