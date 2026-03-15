import { MongoClient } from 'mongodb';

function serializeValue(value) {
  if (value === null || value === undefined) return null;
  if (Buffer.isBuffer(value)) return value.toString('utf8');
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
  const uri = config.uri || `mongodb://${config.username}:${encodeURIComponent(config.password)}@${config.host}:${config.port}/${config.database}`;
  const client = new MongoClient(uri, { connectTimeoutMS: 10000, serverSelectionTimeoutMS: 10000 });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.close();
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
