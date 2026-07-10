function WarningIcon() {
  return (
    <svg className="state-icon" viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M12 3.5 21 19H3L12 3.5Z"
        fill="none"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.8"
      />
      <path
        d="M12 8.5v5"
        fill="none"
        stroke="currentColor"
        strokeLinecap="round"
        strokeWidth="1.8"
      />
      <circle cx="12" cy="16.8" r="0.9" fill="currentColor" />
    </svg>
  );
}

export default function StatusState({
  title = 'No hay datos disponibles',
  message = 'Intenta actualizar la conexión o cambiar el rango de fechas.',
  actionLabel = 'Reintentar',
  onRetry,
}) {
  return (
    <div className="empty-state" role="status">
      <WarningIcon />
      <div>
        <h3>{title}</h3>
        <p>{message}</p>
      </div>
      {onRetry && (
        <button type="button" onClick={onRetry}>
          {actionLabel}
        </button>
      )}
    </div>
  );
}
