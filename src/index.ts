import $ from 'jquery';
import { bitable } from '@lark-base-open/js-sdk';
import './index.scss';
import {
  connectDatabase,
  DbConnectionConfig,
  DbTableMeta,
  DbTablePayload,
  syncDatabaseTables,
  checkDbQuota,
  updateDbUsage,
  createDbCheckoutSession,
  DbQuotaResponse,
  IncrementalConfig,
  createScheduledJob,
  listScheduledJobs,
  deleteScheduledJob,
  runScheduledJob,
  ScheduledJob
} from './db-api';
import { syncTableToBitable } from './db-table-operations';

const LOCAL_SYNC_BASE_URL = (import.meta.env.VITE_SYNC_BASE_URL as string) || 'http://localhost:8787';
const CF_BASE_URL = (import.meta.env.VITE_CF_BASE_URL as string) || '';

let availableTables: DbTableMeta[] = [];
// columns per table, populated after connect
let tableColumnsCache: Record<string, { name: string; dataType: string }[]> = {};
// cached bitable app token (auto-fetched via JS SDK getSelection)
let cachedBitableAppToken: string = '';
// user paid status
let userIsPaid = false;

const INCREMENTAL_STATE_KEY = 'db_sync_incremental_state';

function loadIncrementalState(): Record<string, Record<string, { primaryKey: string; lastValue: unknown }>> {
  try {
    return JSON.parse(localStorage.getItem(INCREMENTAL_STATE_KEY) || '{}');
  } catch (_) {
    return {};
  }
}

function saveIncrementalState(state: Record<string, Record<string, { primaryKey: string; lastValue: unknown }>>) {
  localStorage.setItem(INCREMENTAL_STATE_KEY, JSON.stringify(state));
}

function getDbKey(config: DbConnectionConfig): string {
  return `${config.dbType}:${config.host}:${config.port}:${config.database || config.filePath || config.uri}`;
}

$(function () {
  initializeApp();
});

async function initializeApp() {
  setDefaultConfig();
  bindEvents();
  await checkPaywallStatus();
  await fetchBitableAppToken();
  loadJobList();
}

async function fetchBitableAppToken() {
  try {
    const selection = await bitable.base.getSelection();
    if (selection.baseId) cachedBitableAppToken = selection.baseId;
  } catch (_) {}
}

async function checkPaywallStatus() {
  const $overlay = $('#scheduledPaywall');
  // Default: show paywall, only hide when confirmed paid
  userIsPaid = false;
  if (CF_BASE_URL) {
    try {
      const userId = await getCurrentUserId();
      const quota = await checkDbQuota(CF_BASE_URL, userId);
      userIsPaid = quota.paid;
    } catch (_) {}
  }
  if (userIsPaid) {
    $overlay.hide();
  } else {
    $overlay.show();
  }
}

function setDefaultConfig() {
  $('#dbHost').val('127.0.0.1');
  $('#dbPort').val('3306');
  $('#rowLimit').val('500');
  $('#tablePrefix').val('同步_');
}

function bindEvents() {
  $('#testConnection').on('click', handleTestConnection);
  $('#startSync').on('click', handleStartSync);
  $('#selectAllTables').on('change', handleToggleSelectAll);
  $('#dbType').on('change', handleDbTypeChange);
  $('input[name="syncMode"]').on('change', handleSyncModeChange);
  $('#createJob').on('click', handleCreateScheduledJob);
  $('#paywallUpgradeBtn').on('click', handlePaywallUpgrade);
}

async function handlePaywallUpgrade() {
  try {
    const userId = await getCurrentUserId();
    const checkout = await createDbCheckoutSession(CF_BASE_URL, userId);
    if (checkout.url) {
      openCheckoutInNewTab(checkout.url);
    }
  } catch (_) {
    showJobResult('无法打开支付页面，请稍后再试。', 'error');
  }
}

function handleDbTypeChange() {
  const dbType = String($('#dbType').val() || 'mysql');
  const $standard = $('#standardDbFields');
  const $sqlite = $('#sqliteFields');
  const $mongo = $('#mongoFields');

  $standard.hide();
  $sqlite.hide();
  $mongo.hide();

  // Clear previous table list when switching DB type
  availableTables = [];
  tableColumnsCache = {};
  $('#tableList').empty();
  $('#tableConfigSection').hide();

  switch (dbType) {
    case 'mysql':
      $standard.show();
      $('#dbPort').val('3306');
      break;
    case 'postgresql':
      $standard.show();
      $('#dbPort').val('5432');
      break;
    case 'sqlite':
      $sqlite.show();
      break;
    case 'mongodb':
      $mongo.show();
      break;
  }
}

function handleSyncModeChange() {
  const mode = String($('input[name="syncMode"]:checked').val() || 'full');
  if (mode === 'incremental') {
    renderIncrementalConfig();
    $('#incrementalConfigSection').show();
  } else {
    $('#incrementalConfigSection').hide();
  }
}

function renderIncrementalConfig() {
  const $list = $('#incrementalKeyList');
  $list.empty();
  const selectedTables = getSelectedTables();
  if (!selectedTables.length) {
    $list.append('<div class="incremental-empty">请先勾选要同步的数据表</div>');
    return;
  }
  for (const tableName of selectedTables) {
    const columns = tableColumnsCache[tableName] || [];
    const optionsHtml = columns.map((c) => `<option value="${escapeHtml(c.name)}">${escapeHtml(c.name)} (${escapeHtml(c.dataType)})</option>`).join('');
    $list.append(`
      <div class="incremental-key-item">
        <span class="incremental-table-name">${escapeHtml(tableName)}</span>
        <select class="config-input config-select incremental-pk-select" data-table="${escapeHtml(tableName)}">
          <option value="">-- 选择主键 --</option>
          ${optionsHtml}
        </select>
      </div>
    `);
  }
}

function getDbConfig(): DbConnectionConfig {
  const dbType = String($('#dbType').val() || 'mysql');
  if (dbType === 'sqlite') {
    return {
      dbType,
      host: '',
      port: 0,
      database: '',
      username: '',
      password: '',
      filePath: String($('#sqliteFilePath').val() || '').trim()
    };
  }
  if (dbType === 'mongodb') {
    return {
      dbType,
      host: '',
      port: 27017,
      database: String($('#mongoDatabase').val() || '').trim(),
      username: '',
      password: '',
      uri: String($('#mongoUri').val() || '').trim()
    };
  }
  // mysql, postgresql
  const host = String($('#dbHost').val() || '').trim();
  const port = Number($('#dbPort').val() || 3306);
  const database = String($('#dbDatabase').val() || '').trim();
  const username = String($('#dbUsername').val() || '').trim();
  const password = String($('#dbPassword').val() || '');
  return {
    dbType,
    host,
    port: Number.isFinite(port) ? Math.max(1, Math.min(65535, Math.floor(port))) : 3306,
    database,
    username,
    password
  };
}

function getSelectedTables(): string[] {
  const selected: string[] = [];
  $('input[data-table-checkbox="1"]:checked').each((_, element) => {
    const value = String($(element).val() || '').trim();
    if (value) selected.push(value);
  });
  return selected;
}

function getTablePrefix(): string {
  return String($('#tablePrefix').val() || '').trim();
}

function getRowLimit(): number {
  const n = Number($('#rowLimit').val() || 500);
  if (!Number.isFinite(n)) return 500;
  return Math.max(1, Math.min(5000, Math.floor(n)));
}

function getSyncMode(): string {
  return String($('input[name="syncMode"]:checked').val() || 'full');
}

function getIncrementalConfig(): IncrementalConfig {
  const config: IncrementalConfig = {};
  const dbConfig = getDbConfig();
  const dbKey = getDbKey(dbConfig);
  const state = loadIncrementalState();
  $('.incremental-pk-select').each((_, el) => {
    const $el = $(el);
    const tableName = String($el.data('table') || '');
    const primaryKey = String($el.val() || '');
    if (tableName && primaryKey) {
      const lastValue = state[dbKey]?.[tableName]?.lastValue;
      config[tableName] = { primaryKey, lastValue: lastValue ?? undefined };
    }
  });
  return config;
}

function validateDbConfig(config: DbConnectionConfig) {
  if (config.dbType === 'sqlite') {
    if (!config.filePath) throw new Error('请输入 SQLite 文件路径');
    return;
  }
  if (config.dbType === 'mongodb') {
    if (!config.uri && !config.host) throw new Error('请输入 MongoDB URI');
    if (!config.database) throw new Error('请输入数据库名');
    return;
  }
  if (!config.host) throw new Error('请输入数据库地址');
  if (!config.database) throw new Error('请输入数据库名');
  if (!config.username) throw new Error('请输入用户名');
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderTableList(tables: DbTableMeta[]) {
  const list = $('#tableList');
  list.empty();
  if (!tables.length) {
    list.append('<div class="table-empty">当前数据库没有可同步的数据表</div>');
    $('#tableConfigSection').hide();
    return;
  }
  for (const table of tables) {
    const rowText = table.estimatedRows >= 0 ? `${table.estimatedRows} 行` : '行数未知';
    const itemHtml = `
      <label class="table-item">
        <input data-table-checkbox="1" type="checkbox" value="${escapeHtml(table.tableName)}" />
        <span class="table-name">${escapeHtml(table.tableName)}</span>
        <span class="table-meta">${escapeHtml(rowText)}</span>
      </label>
    `;
    list.append(itemHtml);
  }
  // Re-render incremental config when table selection changes
  $(list).off('change', 'input[data-table-checkbox]').on('change', 'input[data-table-checkbox]', () => {
    if (getSyncMode() === 'incremental') {
      renderIncrementalConfig();
    }
  });
  $('#tableConfigSection').show();
  $('#selectAllTables').prop('checked', false);
}

function handleToggleSelectAll() {
  const checked = Boolean($('#selectAllTables').prop('checked'));
  $('input[data-table-checkbox="1"]').prop('checked', checked);
  if (getSyncMode() === 'incremental') {
    renderIncrementalConfig();
  }
}

async function getCurrentUserId(): Promise<string> {
  try {
    const userId = await bitable.bridge.getBaseUserId();
    if (userId) return String(userId);
  } catch (_) {}
  try {
    const userId = await bitable.bridge.getUserId();
    if (userId) return String(userId);
  } catch (_) {}
  const host = String(window.location.hostname || '').toLowerCase();
  if (host === 'localhost' || host === '127.0.0.1' || host.endsWith('.local')) {
    return 'local_debug_user';
  }
  throw new Error('无法获取当前登录用户信息，请在多维表格内打开插件后重试');
}

async function handleTestConnection() {
  try {
    const config = getDbConfig();
    validateDbConfig(config);
    setConnectLoading(true);
    const dbTypeName = String($('#dbType option:selected').text() || config.dbType);
    updateProgress(8, `正在连接 ${dbTypeName}`);
    const result = await connectDatabase(LOCAL_SYNC_BASE_URL, config);
    availableTables = result.tables || [];

    // Fetch column info for each table (for incremental config)
    tableColumnsCache = {};
    if (availableTables.length > 0) {
      // Do a sync with rowLimit=1 to just get schema info
      try {
        const schemaResult = await syncDatabaseTables(
          LOCAL_SYNC_BASE_URL,
          config,
          availableTables.map((t) => t.tableName),
          1
        );
        if (schemaResult.tables) {
          for (const t of schemaResult.tables) {
            tableColumnsCache[t.tableName] = t.columns;
          }
        }
      } catch (_) {
        // Columns will be empty, incremental config won't have options
      }
    }

    renderTableList(availableTables);
    updateProgress(20, `连接成功，已加载 ${availableTables.length} 张表`);
    showResult(`连接成功，已获取 ${availableTables.length} 张数据表，请勾选后开始同步。`, 'success');
  } catch (error) {
    showResult(`连接失败：${(error as Error).message}`, 'error');
    hideProgress();
  } finally {
    setConnectLoading(false);
  }
}

async function handleStartSync() {
  try {
    const config = getDbConfig();
    validateDbConfig(config);
    const selectedTables = getSelectedTables();
    const rowLimit = getRowLimit();
    const tablePrefix = getTablePrefix();
    const syncMode = getSyncMode();
    const incrementalConfig = syncMode === 'incremental' ? getIncrementalConfig() : {};

    if (!selectedTables.length) {
      throw new Error('请至少选择一张要同步的数据表');
    }
    if (syncMode === 'incremental') {
      const hasPk = selectedTables.some((t) => incrementalConfig[t]?.primaryKey);
      if (!hasPk) {
        throw new Error('增量模式下，请至少为一张表选择主键字段');
      }
    }

    const userId = await getCurrentUserId();

    // Check quota via Cloudflare Worker if configured
    let quotaInfo: DbQuotaResponse | null = null;
    if (CF_BASE_URL) {
      try {
        quotaInfo = await checkDbQuota(CF_BASE_URL, userId);
        if (!quotaInfo.paid && quotaInfo.remaining <= 0) {
          try {
            const checkout = await createDbCheckoutSession(CF_BASE_URL, userId);
            if (checkout.url) {
              const opened = openCheckoutInNewTab(checkout.url);
              if (!opened) {
                showResult('免费额度已用完（3 次），请点击支付后继续使用。', 'info');
                return;
              }
              showResult('免费额度已用完（3 次），已在新窗口打开支付页面。', 'info');
              return;
            }
          } catch (_) {}
          showResult('免费额度已用完（3 次），请购买后继续使用。', 'info');
          return;
        }
      } catch (_) {}
    }

    setSyncLoading(true);
    const dbTypeName = String($('#dbType option:selected').text() || config.dbType);
    updateProgress(15, `正在从 ${dbTypeName} 拉取表结构和数据`);
    const syncResult = await syncDatabaseTables(
      LOCAL_SYNC_BASE_URL,
      config,
      selectedTables,
      rowLimit,
      userId,
      syncMode,
      incrementalConfig
    );
    if (syncResult.status === 'payment_required') {
      const checkoutUrl = String(syncResult.checkoutUrl || '');
      if (checkoutUrl) {
        const opened = openCheckoutInNewTab(checkoutUrl);
        if (!opened) {
          showResult('当前同步额度不足，请点击支付后继续使用。', 'info');
          return;
        }
        showResult('当前同步额度不足，已在新窗口打开支付页面。', 'info');
        return;
      }
      throw new Error(syncResult.message || '额度不足，请先支付');
    }
    const tablePayloadList: DbTablePayload[] = Array.isArray(syncResult.tables) ? syncResult.tables : [];
    if (!tablePayloadList.length) {
      throw new Error('未获取到可同步的数据表内容');
    }
    const summary: string[] = [];
    for (let index = 0; index < tablePayloadList.length; index += 1) {
      const tablePayload = tablePayloadList[index];
      const startProgress = 20 + (index / tablePayloadList.length) * 75;
      const spanProgress = 75 / tablePayloadList.length;
      updateProgress(startProgress, `正在写入 ${tablePayload.tableName}`);
      const result = await syncTableToBitable(
        tablePayload,
        tablePrefix,
        (innerProgress, message) => {
          const next = startProgress + (innerProgress / 100) * spanProgress;
          updateProgress(next, message);
        },
        { syncMode: syncMode as 'full' | 'incremental' }
      );
      summary.push(`${result.tableName}（${result.rowCount} 行）`);

      // Update incremental state
      if (syncMode === 'incremental' && incrementalConfig[tablePayload.tableName]?.primaryKey) {
        const pk = incrementalConfig[tablePayload.tableName].primaryKey;
        if (tablePayload.rows.length > 0) {
          const lastRow = tablePayload.rows[tablePayload.rows.length - 1];
          const state = loadIncrementalState();
          const dbKey = getDbKey(config);
          if (!state[dbKey]) state[dbKey] = {};
          state[dbKey][tablePayload.tableName] = {
            primaryKey: pk,
            lastValue: lastRow[pk]
          };
          saveIncrementalState(state);
        }
      }
    }

    // Update usage after successful sync
    if (CF_BASE_URL && quotaInfo && !quotaInfo.paid) {
      await updateDbUsage(CF_BASE_URL, userId, tablePayloadList.length).catch(() => {});
    }

    updateProgress(100, '同步完成');
    showResult(`同步完成：${summary.join('，')}`, 'success');
  } catch (error) {
    showResult(`同步失败：${(error as Error).message}`, 'error');
    hideProgress();
  } finally {
    setSyncLoading(false);
  }
}

/* ---------- Scheduled Jobs ---------- */

async function handleCreateScheduledJob() {
  try {
    const config = getDbConfig();
    validateDbConfig(config);
    const selectedTables = getSelectedTables();
    if (!selectedTables.length) throw new Error('请先连接数据库并勾选要同步的表');

    // Read token from input, auto-fetch App Token via JS SDK
    const bitableToken = String($('#bitableToken').val() || '').trim();
    if (!bitableToken) throw new Error('请输入多维表格授权码');

    await fetchBitableAppToken();
    if (!cachedBitableAppToken) throw new Error('无法获取多维表格 App Token，请确保在飞书多维表格内使用本插件');

    const cronExpression = String($('#cronExpression').val() || '0 * * * *');
    const jobName = String($('#jobName').val() || '').trim() || '定时同步任务';
    const syncMode = getSyncMode();
    const incrementalConfig = syncMode === 'incremental' ? getIncrementalConfig() : {};

    setCreateJobLoading(true);
    const job = await createScheduledJob(LOCAL_SYNC_BASE_URL, {
      name: jobName,
      dbType: config.dbType,
      host: config.host,
      port: config.port,
      database: config.database,
      username: config.username,
      password: config.password,
      filePath: config.filePath,
      uri: config.uri,
      selectedTables,
      rowLimit: getRowLimit(),
      tablePrefix: getTablePrefix(),
      syncMode,
      incrementalConfig,
      bitableToken,
      bitableAppToken: cachedBitableAppToken,
      cronExpression
    });
    showJobResult(`定时任务创建成功：${job.name}（${job.cronExpression}）`, 'success');
    await loadJobList();
  } catch (error) {
    showJobResult(`创建失败：${(error as Error).message}`, 'error');
  } finally {
    setCreateJobLoading(false);
  }
}

async function loadJobList() {
  try {
    const jobs = await listScheduledJobs(LOCAL_SYNC_BASE_URL);
    renderJobList(jobs);
  } catch (_) {
    // Server might not be running
  }
}

function renderJobList(jobs: ScheduledJob[]) {
  if (!jobs.length) {
    $('#jobListSection').hide();
    return;
  }
  $('#jobListSection').show();
  const $list = $('#jobList');
  $list.empty();
  for (const job of jobs) {
    const statusClass = job.lastStatus === 'success' ? 'job-status-ok' : job.lastStatus === 'failed' ? 'job-status-fail' : 'job-status-pending';
    const statusText = job.lastStatus === 'success' ? '成功' : job.lastStatus === 'failed' ? '失败' : '待执行';
    const lastRunText = job.lastRun ? new Date(job.lastRun).toLocaleString() : '尚未执行';
    $list.append(`
      <div class="job-item" data-job-id="${escapeHtml(job.id)}">
        <div class="job-info">
          <div class="job-name">${escapeHtml(job.name)}</div>
          <div class="job-detail">${escapeHtml(job.dbType)} · ${escapeHtml(job.database)} · ${escapeHtml(job.cronExpression)}</div>
          <div class="job-detail">上次执行: ${escapeHtml(lastRunText)} <span class="${statusClass}">${escapeHtml(statusText)}</span></div>
          ${job.lastError ? `<div class="job-error">${escapeHtml(job.lastError)}</div>` : ''}
        </div>
        <div class="job-actions">
          <button class="job-run-btn" data-job-id="${escapeHtml(job.id)}" title="立即执行"><i class="fas fa-play"></i></button>
          <button class="job-delete-btn" data-job-id="${escapeHtml(job.id)}" title="删除"><i class="fas fa-trash"></i></button>
        </div>
      </div>
    `);
  }

  $list.off('click', '.job-delete-btn').on('click', '.job-delete-btn', async function () {
    const jobId = String($(this).data('job-id') || '');
    if (!jobId) return;
    try {
      await deleteScheduledJob(LOCAL_SYNC_BASE_URL, jobId);
      showJobResult('任务已删除', 'success');
      await loadJobList();
    } catch (error) {
      showJobResult(`删除失败：${(error as Error).message}`, 'error');
    }
  });

  $list.off('click', '.job-run-btn').on('click', '.job-run-btn', async function () {
    const jobId = String($(this).data('job-id') || '');
    if (!jobId) return;
    const $btn = $(this);
    $btn.prop('disabled', true);
    try {
      await runScheduledJob(LOCAL_SYNC_BASE_URL, jobId);
      showJobResult('手动执行成功', 'success');
      await loadJobList();
    } catch (error) {
      showJobResult(`执行失败：${(error as Error).message}`, 'error');
    } finally {
      $btn.prop('disabled', false);
    }
  });
}

/* ---------- UI helpers ---------- */

function openCheckoutInNewTab(checkoutUrl: string): boolean {
  try {
    const nextWindow = window.open(checkoutUrl, '_blank', 'noopener,noreferrer');
    if (nextWindow) return true;
  } catch (_) {}
  // Fallback: show a clickable link for iframe environments where window.open is blocked
  showResult(`请点击以下链接完成支付：<a href="${escapeHtml(checkoutUrl)}" target="_blank" rel="noopener noreferrer">打开支付页面</a>`, 'info');
  return true;
}

function setConnectLoading(loading: boolean) {
  const button = $('#testConnection');
  const text = $('#connectBtnText');
  const spinner = $('#connectLoadingSpinner');
  button.prop('disabled', loading);
  text.text(loading ? '连接中...' : '连接数据库并加载表');
  if (loading) spinner.show();
  else spinner.hide();
}

function setSyncLoading(loading: boolean) {
  const button = $('#startSync');
  const text = $('#syncBtnText');
  const spinner = $('#syncLoadingSpinner');
  button.prop('disabled', loading);
  text.text(loading ? '同步中...' : '确认并同步');
  if (loading) spinner.show();
  else spinner.hide();
}

function setCreateJobLoading(loading: boolean) {
  const button = $('#createJob');
  const text = $('#createJobBtnText');
  const spinner = $('#createJobSpinner');
  button.prop('disabled', loading);
  text.text(loading ? '创建中...' : '创建定时任务');
  if (loading) spinner.show();
  else spinner.hide();
}

function updateProgress(progress: number, message: string) {
  const safeProgress = Math.max(0, Math.min(100, progress));
  $('#syncProgressContainer').show();
  $('#syncProgressBar').css('width', `${safeProgress}%`);
  $('#syncProgressText').text(message);
  $('#syncProgressValue').text(`${Math.round(safeProgress)}%`);
}

function hideProgress() {
  $('#syncProgressContainer').hide();
  $('#syncProgressBar').css('width', '0%');
  $('#syncProgressText').text('');
  $('#syncProgressValue').text('0%');
}

function showResult(message: string, type: 'success' | 'error' | 'info') {
  const messageEl = $('#resultMessage');
  messageEl.removeClass('success error info').addClass(type).html(escapeHtml(message).replace(/\n/g, '<br>'));
  $('#resultContainer').show();
}

function showJobResult(message: string, type: 'success' | 'error' | 'info') {
  const messageEl = $('#jobResultMessage');
  messageEl.removeClass('success error info').addClass(type).html(escapeHtml(message).replace(/\n/g, '<br>'));
  $('#jobResultContainer').show();
  setTimeout(() => { $('#jobResultContainer').hide(); }, 5000);
}
