import { bitable, FieldType, ITable } from '@lark-base-open/js-sdk';
import { DbTablePayload } from './db-api';

type ProgressHandler = (progress: number, message: string) => void;

function mapFieldType(dataType: string): FieldType {
  const type = String(dataType || '').toLowerCase();
  if (
    type.includes('int') ||
    type.includes('decimal') ||
    type.includes('float') ||
    type.includes('double') ||
    type.includes('numeric') ||
    type.includes('real') ||
    type.includes('bit') ||
    type.includes('serial') ||
    type.includes('money') ||
    type === 'number'
  ) {
    return FieldType.Number;
  }
  if (type.includes('date') || type.includes('time') || type.includes('year') || type.includes('datetime') || type.includes('timestamp')) {
    return FieldType.DateTime;
  }
  if (type === 'boolean' || type === 'bool') {
    return FieldType.Text;
  }
  return FieldType.Text;
}

async function ensureTable(tableName: string): Promise<ITable> {
  const allTables = await bitable.base.getTableMetaList();
  const found = allTables.find((table) => table.name === tableName);
  if (found) {
    return bitable.base.getTableById(found.id);
  }
  const created = await bitable.base.addTable({
    name: tableName,
    fields: [{ name: '占位字段', type: FieldType.Text }]
  });
  return bitable.base.getTableById(created.tableId);
}

async function clearTableRecords(table: ITable): Promise<void> {
  const recordIds = await table.getRecordIdList();
  const chunkSize = 100;
  for (let i = 0; i < recordIds.length; i += chunkSize) {
    await table.deleteRecords(recordIds.slice(i, i + chunkSize));
  }
}

async function ensureFields(table: ITable, columns: DbTablePayload['columns']): Promise<Record<string, string>> {
  const fieldMetaList = await table.getFieldMetaList();
  const fieldMap = new Map(fieldMetaList.map((item) => [item.name, item.id]));
  for (const column of columns) {
    if (fieldMap.has(column.name)) continue;
    await table.addField({
      name: column.name,
      type: mapFieldType(column.dataType) as FieldType.Text | FieldType.Number | FieldType.DateTime
    });
  }
  if (!fieldMap.has('同步时间')) {
    await table.addField({
      name: '同步时间',
      type: FieldType.DateTime
    });
  }
  // Clean up the placeholder field created by ensureTable
  if (fieldMap.has('占位字段')) {
    try {
      await table.deleteField(fieldMap.get('占位字段')!);
    } catch (_) {
      // Bitable requires at least one field — ignore if deletion fails
    }
  }
  const latestMeta = await table.getFieldMetaList();
  return latestMeta.reduce<Record<string, string>>((acc, item) => {
    acc[item.name] = item.id;
    return acc;
  }, {});
}

function toCellValue(value: unknown, dataType: string): unknown {
  if (value === null || value === undefined) return '';
  const type = String(dataType || '').toLowerCase();
  if (type.includes('date') || type.includes('time') || type.includes('year') || type.includes('timestamp')) {
    if (value instanceof Date) return value.getTime();
    const time = new Date(String(value)).getTime();
    return Number.isNaN(time) ? '' : time;
  }
  if (
    type.includes('int') ||
    type.includes('decimal') ||
    type.includes('float') ||
    type.includes('double') ||
    type.includes('numeric') ||
    type.includes('real') ||
    type.includes('bit') ||
    type.includes('serial') ||
    type.includes('money') ||
    type === 'number'
  ) {
    const n = Number(value);
    return Number.isFinite(n) ? n : '';
  }
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

export interface SyncTableOptions {
  syncMode?: 'full' | 'incremental';
}

export async function syncTableToBitable(
  tablePayload: DbTablePayload,
  tablePrefix: string,
  onProgress?: ProgressHandler,
  options?: SyncTableOptions
): Promise<{ tableName: string; rowCount: number }> {
  const tableName = `${tablePrefix}${tablePayload.tableName}`;
  const syncMode = options?.syncMode || 'full';

  onProgress?.(10, `准备同步表 ${tablePayload.tableName}`);
  const table = await ensureTable(tableName);

  if (syncMode === 'full') {
    onProgress?.(20, `清理目标表 ${tableName} 历史数据`);
    await clearTableRecords(table);
  } else {
    onProgress?.(20, `增量追加到 ${tableName}`);
  }

  onProgress?.(35, `校验字段 ${tableName}`);
  const fieldIdMap = await ensureFields(table, tablePayload.columns);

  const batchSize = 50;
  const rows = Array.isArray(tablePayload.rows) ? tablePayload.rows : [];
  if (!rows.length) {
    return { tableName, rowCount: 0 };
  }

  const syncTimeFieldId = fieldIdMap['同步时间'];
  const total = rows.length;
  const totalBatch = Math.ceil(total / batchSize);
  for (let batchIndex = 0; batchIndex < totalBatch; batchIndex += 1) {
    const start = batchIndex * batchSize;
    const end = Math.min(start + batchSize, total);
    const batchRows = rows.slice(start, end);
    const records = batchRows.map((row) => {
      const fields: Record<string, any> = {};
      for (const column of tablePayload.columns) {
        const fieldId = fieldIdMap[column.name];
        if (!fieldId) continue;
        fields[fieldId] = toCellValue(row[column.name], column.dataType);
      }
      if (syncTimeFieldId) {
        fields[syncTimeFieldId] = Date.now();
      }
      return { fields };
    });
    await table.addRecords(records as any);
    const progress = 35 + ((batchIndex + 1) / totalBatch) * 65;
    onProgress?.(progress, `已写入 ${end}/${total} 行到 ${tableName}`);
  }
  return { tableName, rowCount: rows.length };
}
