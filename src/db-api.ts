export interface DbConnectionConfig {
  dbType: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  filePath?: string;
  uri?: string;
}

export interface DbTableMeta {
  tableName: string;
  estimatedRows: number;
}

export interface DbColumnMeta {
  name: string;
  dataType: string;
}

export interface DbTablePayload {
  tableName: string;
  columns: DbColumnMeta[];
  rows: Array<Record<string, unknown>>;
}

export interface DbConnectResponse {
  status: 'ok';
  tables: DbTableMeta[];
}

export interface DbSyncResponse {
  status: 'ok' | 'payment_required';
  message?: string;
  checkoutUrl?: string;
  tables?: DbTablePayload[];
}

export interface DbQuotaResponse {
  paid: boolean;
  used: number;
  remaining: number;
  total: number;
}

export interface IncrementalConfig {
  [tableName: string]: { primaryKey: string; lastValue?: unknown };
}

export interface ScheduledJobConfig {
  name: string;
  dbType: string;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
  filePath?: string;
  uri?: string;
  selectedTables: string[];
  rowLimit: number;
  tablePrefix: string;
  syncMode: string;
  incrementalConfig: IncrementalConfig;
  bitableToken: string;
  bitableAppToken: string;
  cronExpression: string;
}

export interface ScheduledJob {
  id: string;
  name: string;
  dbType: string;
  database: string;
  selectedTables: string[];
  syncMode: string;
  cronExpression: string;
  createdAt: string;
  lastRun: string | null;
  lastStatus: string | null;
  lastError: string | null;
}

async function requestJson<T>(url: string, init: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  const text = await response.text();
  let data: any = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch (_) {
    data = {};
  }
  if (!response.ok) {
    const message = String(data?.message || data?.error || text || `请求失败: ${response.status}`);
    throw new Error(message);
  }
  return data as T;
}

export async function connectDatabase(baseUrl: string, config: DbConnectionConfig): Promise<DbConnectResponse> {
  return requestJson<DbConnectResponse>(`${baseUrl}/api/db/connect`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(config)
  });
}

export async function syncDatabaseTables(
  baseUrl: string,
  config: DbConnectionConfig,
  selectedTables: string[],
  rowLimit: number,
  userId?: string,
  syncMode: string = 'full',
  incrementalConfig: IncrementalConfig = {}
): Promise<DbSyncResponse> {
  const payload: Record<string, unknown> = {
    ...config,
    selectedTables,
    rowLimit,
    syncMode,
    incrementalConfig
  };
  if (userId) payload.userId = userId;
  return requestJson<DbSyncResponse>(`${baseUrl}/api/db/sync`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
}

export async function checkDbQuota(cfBaseUrl: string, userId: string): Promise<DbQuotaResponse> {
  return requestJson<DbQuotaResponse>(
    `${cfBaseUrl}/api/db/quota?userId=${encodeURIComponent(userId)}`,
    { method: 'GET' }
  );
}

export async function updateDbUsage(cfBaseUrl: string, userId: string, count: number): Promise<void> {
  await requestJson<{ status: string }>(
    `${cfBaseUrl}/api/db/usage`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId, count })
    }
  );
}

export async function createDbCheckoutSession(cfBaseUrl: string, userId: string): Promise<{ url: string }> {
  const result = await requestJson<{ status: string; url: string }>(
    `${cfBaseUrl}/api/stripe/create-checkout-session`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        userId,
        successUrl: window.location.href,
        cancelUrl: window.location.href
      })
    }
  );
  return { url: result.url };
}

export async function createScheduledJob(
  baseUrl: string,
  config: ScheduledJobConfig
): Promise<{ id: string; name: string; cronExpression: string }> {
  const result = await requestJson<{ status: string; job: { id: string; name: string; cronExpression: string } }>(
    `${baseUrl}/api/jobs`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config)
    }
  );
  return result.job;
}

export async function listScheduledJobs(baseUrl: string): Promise<ScheduledJob[]> {
  const result = await requestJson<{ status: string; jobs: ScheduledJob[] }>(
    `${baseUrl}/api/jobs`,
    { method: 'GET' }
  );
  return result.jobs || [];
}

export async function deleteScheduledJob(baseUrl: string, jobId: string): Promise<void> {
  await requestJson<{ status: string }>(
    `${baseUrl}/api/jobs/${encodeURIComponent(jobId)}`,
    { method: 'DELETE' }
  );
}

export async function runScheduledJob(baseUrl: string, jobId: string): Promise<void> {
  await requestJson<{ status: string }>(
    `${baseUrl}/api/jobs/${encodeURIComponent(jobId)}/run`,
    { method: 'POST' }
  );
}
