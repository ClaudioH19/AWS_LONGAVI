import { useEffect, useMemo, useState } from 'react';
import { buildExportUrl, fetchWeatherRange } from '../api/weatherApi';

function getToday() {
  return new Date().toISOString().split('T')[0];
}

function isNumericValue(value) {
  return value !== null && value !== '' && !Number.isNaN(Number(value));
}

export default function DataTable({ refreshTick = 0 }) {
  const [filters, setFilters] = useState({
    desde: '',
    hasta: getToday(),
    limit: 100,
  });
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const orderedColumns = useMemo(() => {
    if (!rows.length) return [];
    const pinned = ['id', 'received_at'];
    const set = new Set();
    rows.forEach((row) => Object.keys(row).forEach((key) => set.add(key)));
    const all = Array.from(set);
    const rest = all.filter((key) => !pinned.includes(key));
    return [...pinned.filter((key) => all.includes(key)), ...rest];
  }, [rows]);

  async function loadTable() {
    setLoading(true);
    setError('');
    try {
      const data = await fetchWeatherRange(filters);
      setRows(data);
    } catch (e) {
      setError('No se pudieron cargar los datos de la tabla.');
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
    setFilters({ desde: '', hasta: getToday(), limit: 100 });
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
      <h2>Tabla de datos</h2>
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
        <button onClick={loadTable} disabled={loading}>{loading ? 'Cargando...' : 'Filtrar'}</button>
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
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={`${row.id}-${row.received_at}`}>
                  {orderedColumns.map((column) => {
                    const value = row[column] ?? '';
                    const className =
                      column === 'received_at' || column === 'Timestamp'
                        ? 'ts'
                        : column === 'DeviceID' || column === 'DeviceType'
                          ? 'device'
                          : isNumericValue(value)
                            ? 'numeric'
                            : '';
                    return (
                      <td key={`${row.id}-${column}`} className={className} title={String(value)}>
                        {String(value)}
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
