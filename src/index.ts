import $ from 'jquery';
import { bitable } from '@lark-base-open/js-sdk';
import './index.scss';
import { t, detectLocale, setLocale, getLocale, applyI18n, isChineseLocale } from './i18n';
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
  ScheduledJob,
  createAlipayOrder,
  queryAlipayOrder,
  getEntitlement
} from './db-api';
import { syncTableToBitable } from './db-table-operations';

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string) || 'https://dbsync.xiaomiao.win';

let availableTables: DbTableMeta[] = [];
// columns per table, populated after connect
let tableColumnsCache: Record<string, { name: string; dataType: string }[]> = {};
// cached bitable app token (auto-fetched via JS SDK getSelection)
let cachedBitableAppToken: string = '';
// user paid status
let userIsPaid = false;
// Alipay polling timer
let alipayPollTimer: ReturnType<typeof setInterval> | null = null;

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
  return `${config.dbType}:${config.host}:${config.port}:${config.database || config.uri}`;
}

$(function () {
  initializeApp();
});

async function initializeApp() {
  // Detect and apply locale before anything else
  const locale = await detectLocale();
  setLocale(locale);
  applyI18n();

  setDefaultConfig();
  bindEvents();
  await checkPaywallStatus();
  await checkRenewalStatus();
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
  if (API_BASE_URL) {
    try {
      const userId = await getCurrentUserId();
      const quota = await checkDbQuota(API_BASE_URL, userId);
      userIsPaid = quota.paid;
    } catch (_) {}
  }
  if (userIsPaid) {
    $overlay.hide();
  } else {
    $overlay.show();
  }
}

function formatDate(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

const RENEWAL_WARN_DAYS = 7;

async function checkRenewalStatus() {
  if (!API_BASE_URL || !userIsPaid) return;
  try {
    const userId = await getCurrentUserId();
    const resp = await getEntitlement(API_BASE_URL, userId);
    const expiresAt = resp.entitlement?.expiresAt;
    if (!expiresAt || expiresAt <= 0) return;

    const now = Date.now();
    const daysLeft = Math.ceil((expiresAt - now) / (1000 * 60 * 60 * 24));
    const expiryDate = formatDate(new Date(expiresAt));

    if (daysLeft <= 0) {
      // Already expired
      $('#renewalBannerText').text(t('renewal.expired', { date: expiryDate }));
      $('#renewalBanner').show().addClass('renewal-expired');
    } else if (daysLeft <= RENEWAL_WARN_DAYS) {
      // Expiring soon
      $('#renewalBannerText').text(t('renewal.expiringSoon', { days: daysLeft, date: expiryDate }));
      $('#renewalBanner').show().addClass('renewal-warning');
    }
  } catch (_) {}
}

function setDefaultConfig() {
  $('#dbHost').val('127.0.0.1');
  $('#dbPort').val('3306');
  $('#rowLimit').val('500');
  $('#tablePrefix').val(t('placeholder.tablePrefix'));
}

function bindEvents() {
  $('#testConnection').on('click', handleTestConnection);
  $('#startSync').on('click', handleStartSync);
  $('#selectAllTables').on('change', handleToggleSelectAll);
  $('#dbType').on('change', handleDbTypeChange);
  $('input[name="syncMode"]').on('change', handleSyncModeChange);
  $('#createJob').on('click', handleCreateScheduledJob);
  $('#paywallUpgradeBtn').on('click', handlePaywallUpgrade);
  // Alipay modal
  $('#alipayModalClose').on('click', closeAlipayModal);
  $('.alipay-modal-backdrop').on('click', closeAlipayModal);
  // Renewal banner
  $('#renewalBannerBtn').on('click', handlePaywallUpgrade);
}

async function handlePaywallUpgrade() {
  try {
    const userId = await getCurrentUserId();
    if (isChineseLocale()) {
      await openAlipayPayment(userId);
    } else {
      const checkout = await createDbCheckoutSession(API_BASE_URL, userId);
      if (checkout.url) {
        openCheckoutInNewTab(checkout.url);
      }
    }
  } catch (_) {
    showJobResult(t('msg.paymentFailed'), 'error');
  }
}

function handleDbTypeChange() {
  const dbType = String($('#dbType').val() || 'mysql');
  const $standard = $('#standardDbFields');
  const $mongo = $('#mongoFields');

  $standard.hide();
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
    $list.append(`<div class="incremental-empty">${escapeHtml(t('sync.incrementalEmpty'))}</div>`);
    return;
  }
  for (const tableName of selectedTables) {
    const columns = tableColumnsCache[tableName] || [];
    const optionsHtml = columns.map((c) => `<option value="${escapeHtml(c.name)}">${escapeHtml(c.name)} (${escapeHtml(c.dataType)})</option>`).join('');
    $list.append(`
      <div class="incremental-key-item">
        <span class="incremental-table-name">${escapeHtml(tableName)}</span>
        <select class="config-input config-select incremental-pk-select" data-table="${escapeHtml(tableName)}">
          <option value="">${escapeHtml(t('sync.incrementalSelectPk'))}</option>
          ${optionsHtml}
        </select>
      </div>
    `);
  }
}

function getDbConfig(): DbConnectionConfig {
  const dbType = String($('#dbType').val() || 'mysql');
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
  if (config.dbType === 'mongodb') {
    if (!config.uri && !config.host) throw new Error(t('validate.mongoUri'));
    if (!config.database) throw new Error(t('validate.dbName'));
    return;
  }
  if (!config.host) throw new Error(t('validate.dbHost'));
  if (!config.database) throw new Error(t('validate.dbName'));
  if (!config.username) throw new Error(t('validate.dbUser'));
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
    list.append(`<div class="table-empty">${escapeHtml(t('table.empty'))}</div>`);
    $('#tableConfigSection').hide();
    return;
  }
  for (const table of tables) {
    const rowText = table.estimatedRows >= 0
      ? t('table.rows', { count: table.estimatedRows })
      : t('table.rowsUnknown');
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
  throw new Error(t('msg.userIdError'));
}

async function handleTestConnection() {
  try {
    const config = getDbConfig();
    validateDbConfig(config);
    setConnectLoading(true);
    const dbTypeName = String($('#dbType option:selected').text() || config.dbType);
    updateProgress(8, t('msg.connectingDb', { dbType: dbTypeName }));
    const result = await connectDatabase(API_BASE_URL, config);
    availableTables = result.tables || [];

    // Fetch column info for each table (for incremental config)
    tableColumnsCache = {};
    if (availableTables.length > 0) {
      // Do a sync with rowLimit=1 to just get schema info
      try {
        const schemaResult = await syncDatabaseTables(
          API_BASE_URL,
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
    updateProgress(20, t('msg.connectLoaded', { count: availableTables.length }));
    showResult(t('msg.connectSuccess', { count: availableTables.length }), 'success');
  } catch (error) {
    showResult(t('msg.connectFailed', { error: (error as Error).message }), 'error');
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
      throw new Error(t('msg.selectAtLeastOne'));
    }
    if (syncMode === 'incremental') {
      const hasPk = selectedTables.some((t) => incrementalConfig[t]?.primaryKey);
      if (!hasPk) {
        throw new Error(t('msg.incrementalNoPk'));
      }
    }

    const userId = await getCurrentUserId();

    // Check quota via Cloudflare Worker if configured
    let quotaInfo: DbQuotaResponse | null = null;
    if (API_BASE_URL) {
      try {
        quotaInfo = await checkDbQuota(API_BASE_URL, userId);
        if (!quotaInfo.paid && quotaInfo.remaining <= 0) {
          // Route to appropriate payment method
          if (isChineseLocale()) {
            await openAlipayPayment(userId);
            return;
          }
          try {
            const checkout = await createDbCheckoutSession(API_BASE_URL, userId);
            if (checkout.url) {
              const opened = openCheckoutInNewTab(checkout.url);
              if (!opened) {
                showResult(t('msg.quotaExhaustedClick'), 'info');
                return;
              }
              showResult(t('msg.quotaExhaustedNewTab'), 'info');
              return;
            }
          } catch (_) {}
          showResult(t('msg.quotaExhausted'), 'info');
          return;
        }
      } catch (_) {}
    }

    setSyncLoading(true);
    const dbTypeName = String($('#dbType option:selected').text() || config.dbType);
    updateProgress(15, t('msg.fetchingData', { dbType: dbTypeName }));
    const syncResult = await syncDatabaseTables(
      API_BASE_URL,
      config,
      selectedTables,
      rowLimit,
      userId,
      syncMode,
      incrementalConfig
    );
    if (syncResult.status === 'payment_required') {
      const checkoutUrl = String(syncResult.checkoutUrl || '');
      if (isChineseLocale()) {
        await openAlipayPayment(userId);
        return;
      }
      if (checkoutUrl) {
        const opened = openCheckoutInNewTab(checkoutUrl);
        if (!opened) {
          showResult(t('msg.quotaInsufficient'), 'info');
          return;
        }
        showResult(t('msg.quotaInsufficientNewTab'), 'info');
        return;
      }
      throw new Error(syncResult.message || t('msg.quotaInsufficientGeneric'));
    }
    const tablePayloadList: DbTablePayload[] = Array.isArray(syncResult.tables) ? syncResult.tables : [];
    if (!tablePayloadList.length) {
      throw new Error(t('msg.noSyncTables'));
    }
    const summary: string[] = [];
    for (let index = 0; index < tablePayloadList.length; index += 1) {
      const tablePayload = tablePayloadList[index];
      const startProgress = 20 + (index / tablePayloadList.length) * 75;
      const spanProgress = 75 / tablePayloadList.length;
      updateProgress(startProgress, t('msg.writingTable', { tableName: tablePayload.tableName }));
      const result = await syncTableToBitable(
        tablePayload,
        tablePrefix,
        (innerProgress, message) => {
          const next = startProgress + (innerProgress / 100) * spanProgress;
          updateProgress(next, message);
        },
        { syncMode: syncMode as 'full' | 'incremental' }
      );
      summary.push(t('msg.tableResult', { tableName: result.tableName, rowCount: result.rowCount }));

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
    if (API_BASE_URL && quotaInfo && !quotaInfo.paid) {
      await updateDbUsage(API_BASE_URL, userId, tablePayloadList.length).catch(() => {});
    }

    updateProgress(100, t('msg.syncComplete'));
    const joiner = getLocale() === 'zh' ? '，' : ', ';
    showResult(t('msg.syncCompleteDetail', { summary: summary.join(joiner) }), 'success');
  } catch (error) {
    showResult(t('msg.syncFailed', { error: (error as Error).message }), 'error');
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
    if (!selectedTables.length) throw new Error(t('validate.noTablesSelected'));

    // Read token from input, auto-fetch App Token via JS SDK
    const bitableToken = String($('#bitableToken').val() || '').trim();
    if (!bitableToken) throw new Error(t('validate.bitableToken'));

    await fetchBitableAppToken();
    if (!cachedBitableAppToken) throw new Error(t('validate.bitableAppToken'));

    const cronExpression = String($('#cronExpression').val() || '0 * * * *');
    const jobName = String($('#jobName').val() || '').trim() || t('scheduled.defaultJobName');
    const syncMode = getSyncMode();
    const incrementalConfig = syncMode === 'incremental' ? getIncrementalConfig() : {};

    setCreateJobLoading(true);
    const job = await createScheduledJob(API_BASE_URL, {
      name: jobName,
      dbType: config.dbType,
      host: config.host,
      port: config.port,
      database: config.database,
      username: config.username,
      password: config.password,
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
    showJobResult(t('msg.jobCreateSuccess', { name: job.name, cron: job.cronExpression }), 'success');
    await loadJobList();
  } catch (error) {
    showJobResult(t('msg.jobCreateFailed', { error: (error as Error).message }), 'error');
  } finally {
    setCreateJobLoading(false);
  }
}

async function loadJobList() {
  try {
    const jobs = await listScheduledJobs(API_BASE_URL);
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
    const statusText = job.lastStatus === 'success' ? t('job.statusSuccess') : job.lastStatus === 'failed' ? t('job.statusFailed') : t('job.statusPending');
    const lastRunText = job.lastRun ? new Date(job.lastRun).toLocaleString() : t('job.neverRun');
    $list.append(`
      <div class="job-item" data-job-id="${escapeHtml(job.id)}">
        <div class="job-info">
          <div class="job-name">${escapeHtml(job.name)}</div>
          <div class="job-detail">${escapeHtml(job.dbType)} · ${escapeHtml(job.database)} · ${escapeHtml(job.cronExpression)}</div>
          <div class="job-detail">${escapeHtml(t('job.lastRun'))}: ${escapeHtml(lastRunText)} <span class="${statusClass}">${escapeHtml(statusText)}</span></div>
          ${job.lastError ? `<div class="job-error">${escapeHtml(job.lastError)}</div>` : ''}
        </div>
        <div class="job-actions">
          <button class="job-run-btn" data-job-id="${escapeHtml(job.id)}" title="${escapeHtml(t('job.runTitle'))}"><i class="fas fa-play"></i></button>
          <button class="job-delete-btn" data-job-id="${escapeHtml(job.id)}" title="${escapeHtml(t('job.deleteTitle'))}"><i class="fas fa-trash"></i></button>
        </div>
      </div>
    `);
  }

  $list.off('click', '.job-delete-btn').on('click', '.job-delete-btn', async function () {
    const jobId = String($(this).data('job-id') || '');
    if (!jobId) return;
    try {
      await deleteScheduledJob(API_BASE_URL, jobId);
      showJobResult(t('job.deleted'), 'success');
      await loadJobList();
    } catch (error) {
      showJobResult(t('msg.jobDeleteFailed', { error: (error as Error).message }), 'error');
    }
  });

  $list.off('click', '.job-run-btn').on('click', '.job-run-btn', async function () {
    const jobId = String($(this).data('job-id') || '');
    if (!jobId) return;
    const $btn = $(this);
    $btn.prop('disabled', true);
    try {
      await runScheduledJob(API_BASE_URL, jobId);
      showJobResult(t('job.manualRunSuccess'), 'success');
      await loadJobList();
    } catch (error) {
      showJobResult(t('msg.jobRunFailed', { error: (error as Error).message }), 'error');
    } finally {
      $btn.prop('disabled', false);
    }
  });
}

/* ---------- Alipay Payment ---------- */

async function openAlipayPayment(userId: string) {
  const $modal = $('#alipayModal');
  const $qrCode = $('#alipayQrCode');
  const $loading = $('#alipayQrLoading');
  const $status = $('#alipayStatus');
  const $amount = $('#alipayAmount');
  const $period = $('#alipayPlanPeriod');

  $modal.show();
  $qrCode.empty();
  $loading.show();
  $status.text(t('alipay.generating'));
  $amount.text('');

  // Calculate and display subscription period
  const startDate = new Date();
  const endDate = new Date(startDate);
  endDate.setFullYear(endDate.getFullYear() + 1);
  $period.html(`<i class="fas fa-clock"></i> ${t('alipay.period', { start: formatDate(startDate), end: formatDate(endDate) })}`);

  try {
    const order = await createAlipayOrder(API_BASE_URL, userId);
    $loading.hide();
    $amount.html(`<span class="alipay-amount-value">¥${order.totalAmount}</span><span class="alipay-amount-unit"> / ${t('alipay.duration')}</span>`);
    $status.text(t('alipay.scanToPay'));

    // Generate QR code
    renderQrCode($qrCode[0], order.qrCode);

    // Start polling for payment status
    startAlipayPolling(order.outTradeNo, formatDate(endDate));
  } catch (error) {
    $loading.hide();
    $status.text(t('msg.paymentFailed'));
  }
}

function renderQrCode(container: HTMLElement, url: string) {
  // Use an img tag with QR code API for simplicity
  const qrImgUrl = `https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=${encodeURIComponent(url)}`;
  const img = document.createElement('img');
  img.src = qrImgUrl;
  img.alt = 'Alipay QR Code';
  img.width = 200;
  img.height = 200;
  img.style.borderRadius = '8px';
  container.appendChild(img);
}

function startAlipayPolling(outTradeNo: string, expiryDate: string) {
  stopAlipayPolling();
  let attempts = 0;
  const maxAttempts = 120; // 2 minutes at 1s interval

  alipayPollTimer = setInterval(async () => {
    attempts++;
    if (attempts > maxAttempts) {
      stopAlipayPolling();
      $('#alipayStatus').text(t('alipay.timeout'));
      return;
    }
    try {
      const result = await queryAlipayOrder(API_BASE_URL, outTradeNo);
      if (result.tradeStatus === 'TRADE_SUCCESS') {
        stopAlipayPolling();
        $('#alipayStatus').text(t('alipay.paySuccess', { expiry: expiryDate })).addClass('alipay-status-success');
        userIsPaid = true;
        $('#scheduledPaywall').hide();
        $('#renewalBanner').hide();
        setTimeout(() => closeAlipayModal(), 2000);
      } else if (result.tradeStatus === 'TRADE_CLOSED') {
        stopAlipayPolling();
        $('#alipayStatus').text(t('alipay.tradeClosed'));
      }
    } catch (_) {
      // Ignore polling errors, will retry
    }
  }, 1000);
}

function stopAlipayPolling() {
  if (alipayPollTimer) {
    clearInterval(alipayPollTimer);
    alipayPollTimer = null;
  }
}

function closeAlipayModal() {
  stopAlipayPolling();
  $('#alipayModal').hide();
  $('#alipayQrCode').empty();
  $('#alipayStatus').removeClass('alipay-status-success');
}

/* ---------- UI helpers ---------- */

function openCheckoutInNewTab(checkoutUrl: string): boolean {
  try {
    const nextWindow = window.open(checkoutUrl, '_blank', 'noopener,noreferrer');
    if (nextWindow) return true;
  } catch (_) {}
  // Fallback: show a clickable link for iframe environments where window.open is blocked
  showResult(t('msg.openPaymentLink', { url: checkoutUrl }), 'info');
  return true;
}

function setConnectLoading(loading: boolean) {
  const button = $('#testConnection');
  const text = $('#connectBtnText');
  const spinner = $('#connectLoadingSpinner');
  button.prop('disabled', loading);
  text.text(loading ? t('btn.connecting') : t('btn.connect'));
  if (loading) spinner.show();
  else spinner.hide();
}

function setSyncLoading(loading: boolean) {
  const button = $('#startSync');
  const text = $('#syncBtnText');
  const spinner = $('#syncLoadingSpinner');
  button.prop('disabled', loading);
  text.text(loading ? t('btn.syncing') : t('btn.sync'));
  if (loading) spinner.show();
  else spinner.hide();
}

function setCreateJobLoading(loading: boolean) {
  const button = $('#createJob');
  const text = $('#createJobBtnText');
  const spinner = $('#createJobSpinner');
  button.prop('disabled', loading);
  text.text(loading ? t('btn.creatingJob') : t('btn.createJob'));
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
