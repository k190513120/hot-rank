import express from 'express';
import cors from 'cors';
import { getDriver } from './drivers/index.js';
import { createJob, deleteJob, listJobs, getJobById, executeJob, restoreScheduledJobs } from './scheduler.js';

const app = express();
const port = Number(process.env.PORT || 8787) || 8787;
const host = process.env.HOST || '0.0.0.0';
const baseUrl = process.env.PUBLIC_BASE_URL || `http://localhost:${port}`;
const defaultRowLimit = Number(process.env.DB_DEFAULT_ROW_LIMIT || 500);
const maxRowLimit = Number(process.env.DB_MAX_ROW_LIMIT || 5000);

app.use(cors());
app.use(express.json({ limit: '1mb' }));

/* ---------- helpers ---------- */

function normalizeRowLimit(value, fallback = defaultRowLimit) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.min(maxRowLimit, Math.floor(n)));
}

function normalizeDbConfig(body) {
  const dbType = String(body?.dbType || 'mysql').toLowerCase();
  const config = {
    dbType,
    host: String(body?.host || '').trim(),
    port: Number(body?.port || (dbType === 'postgresql' ? 5432 : dbType === 'mongodb' ? 27017 : 3306)),
    database: String(body?.database || '').trim(),
    username: String(body?.username || body?.user || '').trim(),
    password: String(body?.password || ''),
    filePath: String(body?.filePath || '').trim(),
    uri: String(body?.uri || '').trim()
  };
  return config;
}

function validateDbConfig(config) {
  if (config.dbType === 'sqlite') {
    if (!config.filePath && !config.database) throw new Error('SQLite 文件路径不能为空');
    return;
  }
  if (config.dbType === 'mongodb') {
    if (!config.uri && !config.host) throw new Error('MongoDB 地址或 URI 不能为空');
    if (!config.database) throw new Error('数据库名不能为空');
    return;
  }
  // mysql, postgresql
  if (!config.host) throw new Error('数据库地址不能为空');
  if (!config.database) throw new Error('数据库名不能为空');
  if (!config.username) throw new Error('用户名不能为空');
}

function normalizeSelectedTables(value) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.map((t) => String(t || '').trim()).filter(Boolean))];
}

/* ---------- shared handlers ---------- */

async function handleConnect(req, res) {
  try {
    const config = normalizeDbConfig(req.body);
    validateDbConfig(config);
    const driver = getDriver(config.dbType);
    const tables = await driver.listTables(config);
    res.json({ status: 'ok', tables });
  } catch (error) {
    res.status(400).json({ status: 'failed', message: error.message || '连接数据库失败' });
  }
}

async function handleSync(req, res) {
  try {
    const config = normalizeDbConfig(req.body);
    validateDbConfig(config);
    const selectedTables = normalizeSelectedTables(req.body?.selectedTables);
    const rowLimit = normalizeRowLimit(req.body?.rowLimit);
    const syncMode = String(req.body?.syncMode || 'full');
    const incrementalConfig = req.body?.incrementalConfig || {};

    if (!selectedTables.length) {
      return res.status(400).json({ status: 'failed', message: '至少选择一张数据表' });
    }

    const driver = getDriver(config.dbType);
    const tablePayload = [];

    for (const tableName of selectedTables) {
      const incrementalOpts = {};
      if (syncMode === 'incremental' && incrementalConfig[tableName]) {
        const pk = incrementalConfig[tableName].primaryKey;
        if (pk) {
          incrementalOpts.primaryKey = pk;
          if (incrementalConfig[tableName].lastValue != null) {
            incrementalOpts.lastValue = incrementalConfig[tableName].lastValue;
          }
        }
      }
      const data = await driver.fetchTableData(
        config,
        tableName,
        rowLimit,
        incrementalOpts.primaryKey ? incrementalOpts : undefined
      );
      if (data.columns.length) tablePayload.push(data);
    }

    res.json({ status: 'ok', tables: tablePayload });
  } catch (error) {
    res.status(400).json({ status: 'failed', message: error.message || '同步数据库数据失败' });
  }
}

/* ---------- health ---------- */

app.get('/healthz', (_, res) => {
  res.json({ ok: true, now: Date.now() });
});

/* ---------- generic db endpoints ---------- */

app.post('/api/db/connect', handleConnect);
app.post('/api/db/sync', handleSync);

/* ---------- mysql compat aliases ---------- */

app.post('/api/mysql/connect', (req, res) => {
  req.body = { ...req.body, dbType: 'mysql' };
  handleConnect(req, res);
});

app.post('/api/mysql/sync', (req, res) => {
  req.body = { ...req.body, dbType: 'mysql' };
  handleSync(req, res);
});

/* ---------- scheduled jobs ---------- */

app.get('/api/jobs', (_, res) => {
  try {
    res.json({ status: 'ok', jobs: listJobs() });
  } catch (error) {
    res.status(500).json({ status: 'failed', message: error.message });
  }
});

app.post('/api/jobs', (req, res) => {
  try {
    const body = req.body;
    if (!body.bitableToken) throw new Error('请输入多维表格授权码');
    if (!body.bitableAppToken) throw new Error('请输入多维表格 App Token');
    if (!body.selectedTables?.length) throw new Error('请选择要同步的数据表');
    const dbConfig = normalizeDbConfig(body);
    validateDbConfig(dbConfig);
    const job = createJob({
      name: body.name,
      dbType: dbConfig.dbType,
      dbConfig,
      selectedTables: normalizeSelectedTables(body.selectedTables),
      rowLimit: normalizeRowLimit(body.rowLimit),
      tablePrefix: String(body.tablePrefix || ''),
      syncMode: String(body.syncMode || 'full'),
      incrementalConfig: body.incrementalConfig || {},
      bitableToken: body.bitableToken,
      bitableAppToken: body.bitableAppToken,
      cronExpression: body.cronExpression || '0 * * * *'
    });
    res.json({ status: 'ok', job: { id: job.id, name: job.name, cronExpression: job.cronExpression } });
  } catch (error) {
    res.status(400).json({ status: 'failed', message: error.message });
  }
});

app.delete('/api/jobs/:jobId', (req, res) => {
  try {
    deleteJob(req.params.jobId);
    res.json({ status: 'ok' });
  } catch (error) {
    res.status(400).json({ status: 'failed', message: error.message });
  }
});

app.post('/api/jobs/:jobId/run', async (req, res) => {
  try {
    const job = getJobById(req.params.jobId);
    if (!job) return res.status(404).json({ status: 'failed', message: '任务不存在' });
    const results = await executeJob(job);
    res.json({ status: 'ok', results });
  } catch (error) {
    res.status(400).json({ status: 'failed', message: error.message });
  }
});

/* ---------- start ---------- */

app.listen(port, host, () => {
  console.log(`db sync server is running at ${baseUrl}`);
  restoreScheduledJobs();
});
