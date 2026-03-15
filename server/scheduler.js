import cron from 'node-cron';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { getDriver } from './drivers/index.js';
import { syncTableToBitable } from './feishu-api.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const JOBS_FILE = path.join(__dirname, 'sync-jobs.json');
const STATE_FILE = path.join(__dirname, 'sync-state.json');

// Active cron tasks: { [jobId]: cronTask }
const activeTasks = new Map();

/* ---------- persistence ---------- */

function loadJobs() {
  try {
    if (fs.existsSync(JOBS_FILE)) {
      return JSON.parse(fs.readFileSync(JOBS_FILE, 'utf8'));
    }
  } catch (_) {}
  return [];
}

function saveJobs(jobs) {
  fs.writeFileSync(JOBS_FILE, JSON.stringify(jobs, null, 2), 'utf8');
}

function loadIncrementalState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    }
  } catch (_) {}
  return {};
}

function saveIncrementalState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
}

// Incremental state: { [jobId]: { [tableName]: { primaryKey, lastValue } } }
let incrementalState = loadIncrementalState();

function generateId() {
  return `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

export async function executeJob(job) {
  // Re-read latest job config from disk to avoid stale closures
  const freshJob = getJobById(job.id) || job;
  const driver = getDriver(freshJob.dbType);
  const dbConfig = freshJob.dbConfig;
  const results = [];

  for (const tableName of freshJob.selectedTables) {
    const incrementalOpts = {};
    if (freshJob.syncMode === 'incremental' && freshJob.incrementalConfig?.[tableName]) {
      const pk = freshJob.incrementalConfig[tableName].primaryKey;
      const state = incrementalState[freshJob.id]?.[tableName];
      if (pk) {
        incrementalOpts.primaryKey = pk;
        if (state?.lastValue != null) {
          incrementalOpts.lastValue = state.lastValue;
        }
      }
    }

    const tableData = await driver.fetchTableData(
      dbConfig,
      tableName,
      freshJob.rowLimit || 500,
      incrementalOpts.primaryKey ? incrementalOpts : undefined
    );

    const result = await syncTableToBitable(
      freshJob.bitableToken,
      freshJob.bitableAppToken,
      tableData,
      freshJob.tablePrefix || '',
      freshJob.syncMode || 'full'
    );

    // Update incremental state and persist
    if (freshJob.syncMode === 'incremental' && freshJob.incrementalConfig?.[tableName]) {
      const pk = freshJob.incrementalConfig[tableName].primaryKey;
      if (pk && tableData.rows.length > 0) {
        const lastRow = tableData.rows[tableData.rows.length - 1];
        if (!incrementalState[freshJob.id]) incrementalState[freshJob.id] = {};
        incrementalState[freshJob.id][tableName] = {
          primaryKey: pk,
          lastValue: lastRow[pk]
        };
        saveIncrementalState(incrementalState);
      }
    }

    results.push(result);
  }

  // Update lastRun status
  updateJobStatus(freshJob.id, 'success', null);
  return results;
}

function updateJobStatus(jobId, status, errorMsg) {
  const jobs = loadJobs();
  const idx = jobs.findIndex((j) => j.id === jobId);
  if (idx >= 0) {
    jobs[idx].lastRun = new Date().toISOString();
    jobs[idx].lastStatus = status;
    if (errorMsg) jobs[idx].lastError = errorMsg;
    else delete jobs[idx].lastError;
    saveJobs(jobs);
  }
}

function scheduleJob(job) {
  if (activeTasks.has(job.id)) {
    activeTasks.get(job.id).stop();
  }
  if (!cron.validate(job.cronExpression)) {
    throw new Error(`无效的 cron 表达式: ${job.cronExpression}`);
  }
  const jobId = job.id;
  const task = cron.schedule(job.cronExpression, async () => {
    console.log(`[scheduler] 执行定时任务: ${jobId}`);
    try {
      const results = await executeJob({ id: jobId });
      console.log(`[scheduler] 任务 ${jobId} 完成:`, results.map((r) => `${r.tableName}(${r.rowCount}行)`).join(', '));
    } catch (error) {
      console.error(`[scheduler] 任务 ${jobId} 失败:`, error.message);
      updateJobStatus(jobId, 'failed', error.message);
    }
  });
  activeTasks.set(jobId, task);
}

export function createJob(config) {
  const jobs = loadJobs();
  const job = {
    id: generateId(),
    name: config.name || '定时同步任务',
    dbType: config.dbType || 'mysql',
    dbConfig: config.dbConfig,
    selectedTables: config.selectedTables || [],
    rowLimit: config.rowLimit || 500,
    tablePrefix: config.tablePrefix || '',
    syncMode: config.syncMode || 'full',
    incrementalConfig: config.incrementalConfig || {},
    bitableToken: config.bitableToken,
    bitableAppToken: config.bitableAppToken,
    cronExpression: config.cronExpression || '0 * * * *',
    createdAt: new Date().toISOString(),
    lastRun: null,
    lastStatus: null
  };
  jobs.push(job);
  saveJobs(jobs);
  scheduleJob(job);
  return job;
}

export function deleteJob(jobId) {
  if (activeTasks.has(jobId)) {
    activeTasks.get(jobId).stop();
    activeTasks.delete(jobId);
  }
  delete incrementalState[jobId];
  saveIncrementalState(incrementalState);
  const jobs = loadJobs();
  const filtered = jobs.filter((j) => j.id !== jobId);
  saveJobs(filtered);
}

export function listJobs() {
  return loadJobs().map((job) => ({
    id: job.id,
    name: job.name,
    dbType: job.dbType,
    database: job.dbConfig?.database || job.dbConfig?.filePath || '',
    selectedTables: job.selectedTables,
    syncMode: job.syncMode,
    cronExpression: job.cronExpression,
    createdAt: job.createdAt,
    lastRun: job.lastRun,
    lastStatus: job.lastStatus,
    lastError: job.lastError
  }));
}

export function getJobById(jobId) {
  const jobs = loadJobs();
  return jobs.find((j) => j.id === jobId) || null;
}

export function restoreScheduledJobs() {
  const jobs = loadJobs();
  let count = 0;
  for (const job of jobs) {
    try {
      scheduleJob(job);
      count++;
    } catch (error) {
      console.error(`[scheduler] 恢复任务 ${job.id} 失败:`, error.message);
    }
  }
  if (count > 0) {
    console.log(`[scheduler] 已恢复 ${count} 个定时任务`);
  }
}
