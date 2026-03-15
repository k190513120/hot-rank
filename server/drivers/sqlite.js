import Database from 'better-sqlite3';

function serializeValue(value) {
  if (value === null || value === undefined) return null;
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  if (typeof value === 'bigint') {
    const n = Number(value);
    return Number.isSafeInteger(n) ? n : String(value);
  }
  if (typeof value === 'object') {
    try { return JSON.stringify(value); } catch (_) { return String(value); }
  }
  return value;
}

function withDb(config, fn) {
  const filePath = config.filePath || config.database;
  if (!filePath) throw new Error('SQLite 文件路径不能为空');
  const db = new Database(filePath, { readonly: true });
  try {
    return fn(db);
  } finally {
    db.close();
  }
}

export async function listTables(config) {
  return withDb(config, (db) => {
    const rows = db.prepare(
      `SELECT name AS tableName FROM sqlite_master
       WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
       ORDER BY name ASC`
    ).all();
    return rows.map((r) => {
      let estimatedRows = 0;
      try {
        const countRow = db.prepare(`SELECT COUNT(*) AS cnt FROM "${r.tableName.replace(/"/g, '""')}"`).get();
        estimatedRows = countRow?.cnt || 0;
      } catch (_) {}
      return { tableName: String(r.tableName), estimatedRows };
    });
  });
}

export async function fetchTableData(config, tableName, rowLimit, incrementalOpts) {
  return withDb(config, (db) => {
    const ident = `"${tableName.replace(/"/g, '""')}"`;
    const pragmaRows = db.prepare(`PRAGMA table_info(${ident})`).all();
    const columnList = pragmaRows.map((c) => ({
      name: String(c.name),
      dataType: String(c.type || 'text').toLowerCase()
    }));
    if (!columnList.length) return { tableName, columns: [], rows: [] };

    let query = `SELECT * FROM ${ident}`;
    const params = [];
    if (incrementalOpts?.primaryKey && incrementalOpts?.lastValue != null) {
      const pkIdent = `"${incrementalOpts.primaryKey.replace(/"/g, '""')}"`;
      query += ` WHERE ${pkIdent} > ?`;
      params.push(incrementalOpts.lastValue);
      query += ` ORDER BY ${pkIdent} ASC`;
    }
    query += ` LIMIT ?`;
    params.push(rowLimit);

    const rows = db.prepare(query).all(...params);
    const normalizedRows = rows.map((row) => {
      const record = {};
      for (const key of Object.keys(row || {})) {
        record[key] = serializeValue(row[key]);
      }
      return record;
    });
    return { tableName, columns: columnList, rows: normalizedRows };
  });
}
