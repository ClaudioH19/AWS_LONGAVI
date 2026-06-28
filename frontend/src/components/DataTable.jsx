import { useEffect, useMemo, useState } from 'react';
import { buildExportUrl, fetchWeatherRange } from '../api/weatherApi';
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
  if (column === 'DeviceVersion') return 'Version';
  return getVariableDisplayName(column);
}

export default function DataTable({ refreshTick = 0 }) {
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
    return [...presentPinned, ...presentFixed, ...rest];
  }, [rows]);

  async function loadTable() {
    setLoading(true);
    setError('');
    try {
      const data = await fetchWeatherRange(filters);
      setRows(data);
    } catch {
      setError('No se pudieron cargar los datos historicos.');
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

  function exportFile(format) {
    const url = buildExportUrl(format, filters);
    window.open(url, '_blank', 'noopener,noreferrer');
  }

  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <span className="panel-kicker">Historico</span>
          <h2>Tabla persistida de la estacion</h2>
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
          Limite
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

      {error && <p className="error">{error}</p>}

      {!error && rows.length === 0 && !loading && (
        <p className="muted">Sin datos para el periodo seleccionado.</p>
      )}

      {!error && rows.length > 0 && (
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
