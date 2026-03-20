/**
 * PostgreSQL driver for Cloudflare Workers — uses `postgres` (Porsager).
 * This package has native Cloudflare Workers support and does NOT use eval().
 */
import postgres from 'postgres';

function serializeValue(value) {
  if (value === null || value === undefined) return null;
  if (value instanceof Uint8Array || (typeof Buffer !== 'undefined' && Buffer.isBuffer(value))) {
    return new TextDecoder().decode(value);
  }
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'bigint') {
    const n = Number(value);
    return Number.isSafeInteger(n) ? n : String(value);
  }
  if (typeof value === 'object') {
    try { return JSON.stringify(value); } catch (_) { return String(value); }
  }
  return value;
}

function createSql(config) {
  return postgres({
    host: config.host,
    port: config.port || 5432,
    database: config.database,
    username: config.username,
    password: config.password,
    connect_timeout: 10,
    idle_timeout: 5,
    max: 1,
    fetch_types: false,   // skip pg_type query for faster startup
    prepare: false         // disable prepared statements for compatibility
  });
}

export async function listTables(config) {
  const sql = createSql(config);
  try {
    const rows = await sql`
      SELECT t.table_name AS "tableName",
             COALESCE(s.n_live_tup, 0) AS "estimatedRows"
      FROM information_schema.tables t
      LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
      WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE'
      ORDER BY t.table_name ASC
    `;
    return rows.map((r) => ({
      tableName: String(r.tableName || ''),
      estimatedRows: Number(r.estimatedRows || 0)
    }));
  } finally {
    await sql.end();
  }
}

export async function fetchTableData(config, tableName, rowLimit, incrementalOpts) {
  const sql = createSql(config);
  try {
    // Get column metadata
    const colRows = await sql`
      SELECT column_name AS "columnName", data_type AS "dataType"
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = ${tableName}
      ORDER BY ordinal_position ASC
    `;
    const columnList = colRows.map((c) => ({
      name: String(c.columnName || ''),
      dataType: String(c.dataType || 'text')
    }));
    if (!columnList.length) return { tableName, columns: [], rows: [] };

    // Fetch data — use sql.unsafe for dynamic identifiers
    const limit = Math.max(1, Math.min(10000, Number(rowLimit) || 500));
    let rows;

    if (incrementalOpts?.primaryKey && incrementalOpts?.lastValue != null) {
      const ident = `"${tableName.replace(/"/g, '""')}"`;
      const pkIdent = `"${incrementalOpts.primaryKey.replace(/"/g, '""')}"`;
      rows = await sql.unsafe(
        `SELECT * FROM ${ident} WHERE ${pkIdent} > $1 ORDER BY ${pkIdent} ASC LIMIT $2`,
        [incrementalOpts.lastValue, limit]
      );
    } else {
      const ident = `"${tableName.replace(/"/g, '""')}"`;
      rows = await sql.unsafe(
        `SELECT * FROM ${ident} LIMIT $1`,
        [limit]
      );
    }

    const normalizedRows = rows.map((row) => {
      const record = {};
      for (const key of Object.keys(row)) {
        record[key] = serializeValue(row[key]);
      }
      return record;
    });

    return { tableName, columns: columnList, rows: normalizedRows };
  } finally {
    await sql.end();
  }
}
