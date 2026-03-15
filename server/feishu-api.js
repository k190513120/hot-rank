/**
 * Feishu Bitable API - 使用多维表格授权码（Personal Access Token）访问
 * 文档: https://open.feishu.cn/document/server-docs/docs/bitable-v1
 */

const FEISHU_API_BASE = 'https://open.feishu.cn/open-apis';

async function request(method, path, token, body) {
  const url = `${FEISHU_API_BASE}${path}`;
  const headers = {
    'Authorization': `Bearer ${token}`,
    'Content-Type': 'application/json'
  };
  const options = { method, headers };
  if (body) options.body = JSON.stringify(body);
  const res = await fetch(url, options);
  const data = await res.json();
  if (data.code !== 0) {
    throw new Error(`飞书 API 错误 (${data.code}): ${data.msg || '未知错误'}`);
  }
  return data.data;
}

export async function listBitableTables(token, appToken) {
  const data = await request('GET', `/bitable/v1/apps/${appToken}/tables`, token);
  return (data.items || []).map((t) => ({
    tableId: t.table_id,
    name: t.name
  }));
}

export async function createBitableTable(token, appToken, name, fields) {
  const data = await request('POST', `/bitable/v1/apps/${appToken}/tables`, token, {
    table: { name, default_view_name: '默认视图', fields }
  });
  return data.table_id;
}

export async function listBitableFields(token, appToken, tableId) {
  const data = await request('GET', `/bitable/v1/apps/${appToken}/tables/${tableId}/fields`, token);
  return (data.items || []).map((f) => ({
    fieldId: f.field_id,
    fieldName: f.field_name,
    type: f.type
  }));
}

export async function addBitableField(token, appToken, tableId, fieldName, type) {
  const data = await request('POST', `/bitable/v1/apps/${appToken}/tables/${tableId}/fields`, token, {
    field_name: fieldName,
    type
  });
  // API returns field info directly in data, or nested under data.field
  const field = data.field || data;
  return { field_id: field.field_id, field_name: field.field_name, type: field.type };
}

export function mapToFeishuFieldType(dataType) {
  const t = String(dataType || '').toLowerCase();
  if (t.includes('int') || t.includes('decimal') || t.includes('float') ||
      t.includes('double') || t.includes('numeric') || t.includes('real') ||
      t.includes('number') || t.includes('serial') || t.includes('money')) {
    return 2; // Number
  }
  if (t.includes('date') || t.includes('time') || t.includes('year') || t.includes('datetime') || t.includes('timestamp')) {
    return 5; // DateTime
  }
  return 1; // Text
}

function toCellValue(value, fieldType) {
  if (value === null || value === undefined) return null;
  if (fieldType === 2) { // Number
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  if (fieldType === 5) { // DateTime
    if (value instanceof Date) return value.getTime();
    const time = new Date(String(value)).getTime();
    return Number.isNaN(time) ? null : time;
  }
  return typeof value === 'object' ? JSON.stringify(value) : String(value);
}

export async function batchInsertRecords(token, appToken, tableId, records) {
  const BATCH_SIZE = 450;
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    await request('POST', `/bitable/v1/apps/${appToken}/tables/${tableId}/records/batch_create`, token, {
      records: batch
    });
  }
}

export async function clearBitableRecords(token, appToken, tableId) {
  let pageToken = '';
  const allIds = [];
  do {
    const query = pageToken ? `?page_token=${pageToken}&page_size=500` : '?page_size=500';
    const data = await request('GET', `/bitable/v1/apps/${appToken}/tables/${tableId}/records${query}`, token);
    const items = data.items || [];
    for (const item of items) {
      allIds.push(item.record_id);
    }
    pageToken = data.page_token || '';
  } while (pageToken);

  const BATCH_SIZE = 500;
  for (let i = 0; i < allIds.length; i += BATCH_SIZE) {
    const batch = allIds.slice(i, i + BATCH_SIZE);
    await request('POST', `/bitable/v1/apps/${appToken}/tables/${tableId}/records/batch_delete`, token, {
      records: batch
    });
  }
}

/**
 * 完整同步单张表到 Bitable
 */
export async function syncTableToBitable(token, appToken, tablePayload, tablePrefix, syncMode) {
  const targetName = `${tablePrefix}${tablePayload.tableName}`;

  // Find or create table
  const existingTables = await listBitableTables(token, appToken);
  let targetTable = existingTables.find((t) => t.name === targetName);
  let tableId;

  if (targetTable) {
    tableId = targetTable.tableId;
  } else {
    // Create table — first field must be Text type (Bitable requirement)
    const fields = [];
    for (const col of tablePayload.columns) {
      const fieldType = mapToFeishuFieldType(col.dataType);
      // First field in Bitable must be Text (index field)
      fields.push({
        field_name: col.name,
        type: fields.length === 0 ? 1 : fieldType
      });
    }
    fields.push({ field_name: '同步时间', type: 5 }); // DateTime
    tableId = await createBitableTable(token, appToken, targetName, fields);
  }

  // Ensure fields exist
  const existingFields = await listBitableFields(token, appToken, tableId);
  const fieldNameMap = new Map(existingFields.map((f) => [f.fieldName, f]));

  for (const col of tablePayload.columns) {
    if (!fieldNameMap.has(col.name)) {
      const field = await addBitableField(token, appToken, tableId, col.name, mapToFeishuFieldType(col.dataType));
      fieldNameMap.set(col.name, { fieldId: field.field_id, fieldName: col.name, type: field.type });
    }
  }
  if (!fieldNameMap.has('同步时间')) {
    const field = await addBitableField(token, appToken, tableId, '同步时间', 5);
    fieldNameMap.set('同步时间', { fieldId: field.field_id, fieldName: '同步时间', type: 5 });
  }

  // Refresh field list for accurate IDs
  const allFields = await listBitableFields(token, appToken, tableId);
  const fieldIdMap = {};
  const fieldTypeMap = {};
  for (const f of allFields) {
    fieldIdMap[f.fieldName] = f.fieldId;
    fieldTypeMap[f.fieldName] = f.type;
  }

  // Clear records in full mode
  if (syncMode !== 'incremental') {
    await clearBitableRecords(token, appToken, tableId);
  }

  // Build records
  const now = Date.now();
  const records = tablePayload.rows.map((row) => {
    const fields = {};
    for (const col of tablePayload.columns) {
      const fieldName = col.name;
      if (fieldIdMap[fieldName]) {
        fields[fieldName] = toCellValue(row[fieldName], fieldTypeMap[fieldName] || 1);
      }
    }
    fields['同步时间'] = now;
    return { fields };
  });

  // Batch insert
  await batchInsertRecords(token, appToken, tableId, records);

  return { tableName: targetName, rowCount: tablePayload.rows.length };
}
