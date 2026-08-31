import { useEffect, useMemo, useState } from 'react';
import { buildExportUrl, fetchWeatherRange } from '../api/weatherApi';
import StatusState from './StatusState';
import { formatDateTime, getTodayInChileDateInput } from '../utils/dateTime';
import {
  formatWeatherValue,
  getUnitForKey,
  getVariableDisplayName,
} from '../utils/weatherVariables';

function isNumericValue(value) {
  return value !== null && value !== '' && !Number.isNaN(Number(value));
}

function getColumnLabel(column) {
  if (column === 'id') return 'ID';
  if (column === 'received_at') return 'Recibido en Chile';
  if (column === 'Timestamp') return 'Timestamp';
  if (column === 'DeviceID') return 'Dispositivo';
  if (column === 'DeviceType') return 'Tipo';
  if (column === 'DeviceVersion') return 'Versión';
  return getVariableDisplayName(column);
}

const HIDDEN_UI_COLUMNS = new Set(['id', 'DeviceID', 'DeviceType', 'DeviceVersion', 'Timestamp']);

export default function DataTable({ refreshTick = 0, liveReading = null }) {
  const [filters, setFilters] = useState({
    desde: '',
    hasta: getTodayInChileDateInput(),
    limit: 100,
  });
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const orderedColumns = useMemo(() => {
    if (!rows.length) return [];
    const pinned = ['id', 'received_at', 'DeviceID', 'DeviceType', 'DeviceVersion', 'Timestamp'];
    const fixedVariables = ['Hum', 'Temp', 'Precip', 'Rad', 'Vel', 'Dir'];
    const keys = new Set();
    rows.forEach((row) => Object.keys(row).forEach((key) => keys.add(key)));
    const all = Array.from(keys);
    const presentPinned = pinned.filter((key) => all.includes(key));
    const presentFixed = fixedVariables.filter((key) => all.includes(key));
    const rest = all.filter((key) => !pinned.includes(key) && !fixedVariables.includes(key));
    return [...presentPinned, ...presentFixed, ...rest].filter((key) => !HIDDEN_UI_COLUMNS.has(key));
  }, [rows]);

  async function loadTable() {
    setLoading(true);
    setError('');
    try {
      const data = await fetchWeatherRange(filters);
      setRows(data);
    } catch {
      setError('No se pudieron cargar los datos históricos.');
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  function onFilterChange(event) {
    const { name, value } = event.target;
    setFilters((prev) => ({ ...prev, [name]: value }));
  }

  function clearFilters() {
    setFilters({ desde: '', hasta: getTodayInChileDateInput(), limit: 100 });
  }

  useEffect(() => {
    loadTable();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters, refreshTick]);

  useEffect(() => {
    if (!liveReading?.received_at) return;

    const readingDate = liveReading.received_at.slice(0, 10);
    if ((filters.desde && readingDate < filters.desde) || (filters.hasta && readingDate > filters.hasta)) {
      return;
    }

    setRows((previous) => {
      const alreadyPresent = previous.some((row) => (
        (liveReading.id && row.id === liveReading.id)
        || row.received_at === liveReading.received_at
      ));
      if (alreadyPresent) return previous;

      return [liveReading, ...previous].slice(0, Number(filters.limit) || 100);
    });
  }, [filters.desde, filters.hasta, filters.limit, liveReading]);

  function exportFile(format) {
    const url = buildExportUrl(format, filters);
    window.open(url, '_blank', 'noopener,noreferrer');
  }

  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <span className="panel-kicker">Tabla</span>
          <h2>Histórico de lecturas</h2>
        </div>
      </div>

      <div className="panel-toolbar">
        <label>
          Desde
          <input type="date" name="desde" value={filters.desde} onChange={onFilterChange} />
        </label>
        <label>
          Hasta
          <input type="date" name="hasta" value={filters.hasta} onChange={onFilterChange} />
        </label>
        <label>
          Límite
          <input
            type="number"
            min="1"
            max="5000"
            name="limit"
            value={filters.limit}
            onChange={onFilterChange}
          />
        </label>
        <button type="button" onClick={loadTable} disabled={loading}>
          {loading ? 'Cargando...' : 'Actualizar tabla'}
        </button>
        <button type="button" onClick={clearFilters}>Limpiar</button>
        <button type="button" onClick={() => exportFile('csv')}>Exportar CSV</button>
        <button type="button" onClick={() => exportFile('json')}>Exportar JSON</button>
      </div>

      {error && (
        <StatusState
          title="No se pudo cargar el histórico"
          message="El servidor no respondió o los filtros seleccionados no son válidos."
          onRetry={loadTable}
        />
      )}

      {!error && loading && (
        <div className="table-wrap table-loading" aria-label="Cargando tabla">
          <div className="skeleton-table">
            {Array.from({ length: 7 }).map((_, rowIndex) => (
              <div className="skeleton-row" key={`loading-row-${rowIndex}`}>
                {Array.from({ length: 6 }).map((__, columnIndex) => (
                  <span key={`loading-cell-${rowIndex}-${columnIndex}`} className="skeleton" />
                ))}
              </div>
            ))}
          </div>
        </div>
      )}

      {!error && rows.length === 0 && !loading && (
        <StatusState
          title="No hay datos disponibles"
          message="No se encontraron lecturas para el período seleccionado."
          onRetry={loadTable}
        />
      )}

      {!error && rows.length > 0 && !loading && (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                {orderedColumns.map((column) => (
                  <th key={column}>{getColumnLabel(column)}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={`${row.id}-${row.received_at}`}>
                  {orderedColumns.map((column) => {
                    const value = row[column] ?? '';
                    const hasWeatherUnit = Boolean(getUnitForKey(column));
                    const className =
                      column === 'received_at' || column === 'Timestamp'
                        ? 'ts'
                        : column === 'DeviceID' || column === 'DeviceType'
                          ? 'device'
                          : isNumericValue(value) || hasWeatherUnit
                            ? 'numeric'
                            : '';
                    const displayValue =
                      column === 'received_at' || column === 'Timestamp'
                        ? formatDateTime(value, '')
                        : hasWeatherUnit
                          ? formatWeatherValue(column, value, '')
                        : String(value);

                    return (
                      <td key={`${row.id}-${column}`} className={className} title={String(value)}>
                        {displayValue}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
