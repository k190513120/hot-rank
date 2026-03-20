/**
 * MongoDB driver for Cloudflare Workers.
 *
 * The official `mongodb` npm driver uses eval()/new Function() internally
 * which is blocked in Cloudflare Workers V8 sandbox. This module attempts
 * to use it but provides a clear error message if it fails.
 *
 * For production MongoDB on Cloudflare Workers, consider:
 *   - MongoDB Atlas Data API (HTTP-based, no TCP needed)
 *   - A proxy service that bridges HTTP to MongoDB wire protocol
 */

let MongoClient;
let mongoAvailable = false;

try {
  const mod = await import('mongodb');
  MongoClient = mod.MongoClient;
  mongoAvailable = true;
} catch (_) {
  mongoAvailable = false;
}

function serializeValue(value) {
  if (value === null || value === undefined) return null;
  if (value instanceof Uint8Array || (typeof Buffer !== 'undefined' && Buffer.isBuffer(value))) {
    return new TextDecoder().decode(value);
  }
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'object') {
    if (value._bsontype === 'ObjectId' || (value.constructor && value.constructor.name === 'ObjectId')) {
      return value.toString();
    }
    try { return JSON.stringify(value); } catch (_) { return String(value); }
  }
  return value;
}

async function withClient(config, fn) {
  if (!mongoAvailable || !MongoClient) {
    throw new Error(
      'MongoDB 驱动在 Cloudflare Workers 中不可用（eval() 被禁止）。' +
      '请使用 MongoDB Atlas Data API 或通过代理服务连接。' +
      '\nMongoDB driver is not available in Cloudflare Workers (eval() is blocked). ' +
      'Please use MongoDB Atlas Data API or connect via a proxy service.'
    );
  }
  const uri = config.uri || `mongodb://${config.username}:${encodeURIComponent(config.password)}@${config.host}:${config.port}/${config.database}`;
  const client = new MongoClient(uri, { connectTimeoutMS: 10000, serverSelectionTimeoutMS: 10000 });
  try {
    await client.connect();
    return await fn(client);
  } catch (error) {
    const msg = String(error?.message || '');
    if (msg.includes('Code generation from strings') || msg.includes('eval') || msg.includes('Function')) {
      throw new Error(
        'MongoDB 驱动不兼容 Cloudflare Workers（内部使用了被禁止的 eval/new Function）。' +
        '请使用 MongoDB Atlas Data API 或代理服务。' +
        '\nMongoDB driver is incompatible with Cloudflare Workers (uses eval/new Function internally). ' +
        'Please use MongoDB Atlas Data API or a proxy service.'
      );
    }
    throw error;
  } finally {
    try { await client.close(); } catch (_) {}
  }
}

function inferDataType(value) {
  if (value === null || value === undefined) return 'text';
  if (typeof value === 'number') return 'number';
  if (typeof value === 'boolean') return 'boolean';
  if (value instanceof Date) return 'datetime';
  if (value._bsontype === 'ObjectId' || (value.constructor && value.constructor.name === 'ObjectId')) return 'text';
  return 'text';
}

export async function listTables(config) {
  return withClient(config, async (client) => {
    const db = client.db(config.database);
    const collections = await db.listCollections().toArray();
    const tables = [];
    for (const col of collections) {
      let estimatedRows = 0;
      try {
        estimatedRows = await db.collection(col.name).estimatedDocumentCount();
      } catch (_) {}
      tables.push({ tableName: col.name, estimatedRows });
    }
    tables.sort((a, b) => a.tableName.localeCompare(b.tableName));
    return tables;
  });
}

export async function fetchTableData(config, tableName, rowLimit, incrementalOpts) {
  return withClient(config, async (client) => {
    const db = client.db(config.database);
    const collection = db.collection(tableName);

    const filter = {};
    const sort = {};
    if (incrementalOpts?.primaryKey && incrementalOpts?.lastValue != null) {
      filter[incrementalOpts.primaryKey] = { $gt: incrementalOpts.lastValue };
      sort[incrementalOpts.primaryKey] = 1;
    }

    const docs = await collection.find(filter).sort(sort).limit(rowLimit).toArray();

    // Infer schema from all documents
    const fieldMap = new Map();
    for (const doc of docs) {
      for (const [key, value] of Object.entries(doc)) {
        if (!fieldMap.has(key)) {
          fieldMap.set(key, inferDataType(value));
        }
      }
    }
    // Ensure _id is always first
    const columnList = [];
    if (fieldMap.has('_id')) {
      columnList.push({ name: '_id', dataType: 'text' });
      fieldMap.delete('_id');
    }
    for (const [name, dataType] of fieldMap) {
      columnList.push({ name, dataType });
    }

    const normalizedRows = docs.map((doc) => {
      const record = {};
      for (const key of Object.keys(doc)) {
        record[key] = serializeValue(doc[key]);
      }
      return record;
    });

    return { tableName, columns: columnList, rows: normalizedRows };
  });
}
