import { CHART_NEUTRALS } from './weatherVisualTheme';

function responsiveFont(context, compact = false) {
  const width = context.chart.width;
  if (width >= 760) return { size: compact ? 12 : 13, weight: '600' };
  if (width >= 460) return { size: compact ? 11 : 12, weight: '600' };
  return { size: compact ? 10 : 11, weight: '600' };
}

function formatTooltipValue(value) {
  if (!Number.isFinite(Number(value))) return '--';
  return Number(value).toLocaleString('es-CL', {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });
}

export function buildChartOptions({
  leftAxisTitle,
  rightAxisTitle = '',
  leftBeginAtZero = true,
  rightBeginAtZero = true,
} = {}) {
  const reducedMotion = typeof window !== 'undefined'
    && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    animation: reducedMotion ? false : { duration: 420 },
    scales: {
      x: {
        grid: { color: CHART_NEUTRALS.grid, drawBorder: false },
        ticks: {
          color: CHART_NEUTRALS.text,
          font: (context) => responsiveFont(context, true),
          maxRotation: 0,
          autoSkip: false,
        },
      },
      y: {
        beginAtZero: leftBeginAtZero,
        grid: { color: CHART_NEUTRALS.grid, drawBorder: false },
        ticks: { color: CHART_NEUTRALS.text, font: (context) => responsiveFont(context, true) },
        title: {
          display: Boolean(leftAxisTitle),
          text: leftAxisTitle,
          color: CHART_NEUTRALS.text,
          font: (context) => responsiveFont(context),
        },
      },
      ...(rightAxisTitle ? {
        y1: {
          beginAtZero: rightBeginAtZero,
          position: 'right',
          grid: { drawOnChartArea: false },
          ticks: { color: CHART_NEUTRALS.text, font: (context) => responsiveFont(context, true) },
          title: {
            display: true,
            text: rightAxisTitle,
            color: CHART_NEUTRALS.text,
            font: (context) => responsiveFont(context),
          },
        },
      } : {}),
    },
    plugins: {
      legend: {
        position: 'top',
        align: 'end',
        labels: {
          color: CHART_NEUTRALS.text,
          boxWidth: 10,
          boxHeight: 10,
          usePointStyle: true,
          padding: 14,
          font: (context) => responsiveFont(context, true),
          filter: (item, data) => !data.datasets[item.datasetIndex]?.hideFromLegend,
        },
      },
      tooltip: {
        backgroundColor: CHART_NEUTRALS.tooltip,
        titleColor: '#ffffff',
        bodyColor: '#ffffff',
        borderColor: 'rgba(255, 255, 255, 0.16)',
        borderWidth: 1,
        cornerRadius: 12,
        padding: 12,
        caretPadding: 8,
        filter: (context) => !context.dataset.hideFromTooltip,
        callbacks: {
          title: (contexts) => (contexts.length ? `Fecha: ${contexts[0].label}` : ''),
          label: (context) => `${context.dataset.label}: ${formatTooltipValue(context.parsed.y)}`,
        },
      },
    },
  };
}
