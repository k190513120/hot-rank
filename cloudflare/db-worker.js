/**
 * Cloudflare Worker — 数据库同步插件（合并版）
 *
 * 合并了原 Node.js 服务端（数据库连接、同步、定时任务）和 Worker（配额/支付）。
 * 所有功能现在都在此单一 Worker 中运行：
 *   - 数据库连接 (POST /api/db/connect)
 *   - 数据库同步 (POST /api/db/sync)
 *   - 配额检查 (GET  /api/db/quota)
 *   - 用量上报 (POST /api/db/usage)
 *   - 定时任务 CRUD (GET/POST/DELETE /api/jobs, POST /api/jobs/:id/run)
 *   - Stripe 支付（checkout、webhook、entitlement）
 *   - Cron Trigger 定时执行 (scheduled handler)
 */

import { getDriver } from './drivers/index.js';
import { syncTableToBitable } from './feishu-api.js';

const FREE_DB_QUOTA = 3;
const YEAR_SECONDS = 365 * 24 * 60 * 60;
const DEFAULT_ROW_LIMIT = 500;
const MAX_ROW_LIMIT = 5000;

// ─── helpers ──────────────────────────────────────────────

function corsHeaders(req, env) {
  const requestOrigin = req.headers.get('Origin') || '*';
  const allowOrigin = env.ALLOWED_ORIGIN || requestOrigin || '*';
  return {
    'Access-Control-Allow-Origin': allowOrigin,
    'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    Vary: 'Origin'
  };
}

function jsonResponse(req, env, status, data) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders(req, env) }
  });
}

function normalizeInt(value, fallback, min = 0, max = 1000000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function normalizeUserId(input) {
  return String(input || '').trim();
}

function normalizeEmail(input) {
  return String(input || '').trim().toLowerCase();
}

function normalizeRowLimit(value, fallback = DEFAULT_ROW_LIMIT) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.min(MAX_ROW_LIMIT, Math.floor(n)));
}

function normalizeDbConfig(body) {
  const dbType = String(body?.dbType || 'mysql').toLowerCase();
  return {
    dbType,
    host: String(body?.host || '').trim(),
    port: Number(body?.port || (dbType === 'postgresql' ? 5432 : dbType === 'mongodb' ? 27017 : 3306)),
    database: String(body?.database || '').trim(),
    username: String(body?.username || body?.user || '').trim(),
    password: String(body?.password || ''),
    uri: String(body?.uri || '').trim()
  };
}

function validateDbConfig(config) {
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

function generateId() {
  return `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

// ─── KV state ─────────────────────────────────────────────

async function putState(env, key, value, ttl) {
  if (!env.DB_STATE) return;
  const options = {};
  if (ttl) options.expirationTtl = ttl;
  else options.expirationTtl = 60 * 60 * 24 * 90; // 90 days default
  await env.DB_STATE.put(key, JSON.stringify(value), options);
}

async function getState(env, key) {
  if (!env.DB_STATE) return null;
  const value = await env.DB_STATE.get(key);
  if (!value) return null;
  return JSON.parse(value);
}

async function deleteState(env, key) {
  if (!env.DB_STATE) return;
  await env.DB_STATE.delete(key);
}

// ─── entitlement ──────────────────────────────────────────

function resolveEntitlementActive(entitlement) {
  if (!entitlement || !entitlement.active) return false;
  const expiresAt = Number(entitlement.expiresAt || 0);
  if (!expiresAt) return true;
  return Date.now() < expiresAt;
}

async function getEntitlementByUserId(env, userId) {
  const id = normalizeUserId(userId);
  if (!id) return null;
  return getState(env, `db:entitlement:user:${id}`);
}

async function getEntitlementByEmail(env, email) {
  const e = normalizeEmail(email);
  if (!e) return null;
  return getState(env, `db:entitlement:email:${e}`);
}

async function markEntitlementActive(env, identity, source, expiresAt) {
  const email = normalizeEmail(identity?.email);
  const userId = normalizeUserId(identity?.userId);
  if (!email && !userId) return;
  const resolvedExpiresAt = Number(expiresAt || 0) || Date.now() + YEAR_SECONDS * 1000;
  const payload = { active: true, email: email || '', userId: userId || '', source, expiresAt: resolvedExpiresAt, updatedAt: Date.now() };
  if (email) await putState(env, `db:entitlement:email:${email}`, payload);
  if (userId) await putState(env, `db:entitlement:user:${userId}`, payload);
}

async function markEntitlementInactive(env, identity, source) {
  const email = normalizeEmail(identity?.email);
  const userId = normalizeUserId(identity?.userId);
  if (!email && !userId) return;
  const payload = { active: false, email: email || '', userId: userId || '', source, updatedAt: Date.now() };
  if (email) await putState(env, `db:entitlement:email:${email}`, payload);
  if (userId) await putState(env, `db:entitlement:user:${userId}`, payload);
}

// ─── usage ────────────────────────────────────────────────

async function getUsageByUserId(env, userId) {
  const id = normalizeUserId(userId);
  if (!id) return { usedRecords: 0, userId: id };
  const usage = await getState(env, `db:usage:user:${id}`);
  if (!usage) return { usedRecords: 0, userId: id };
  return { usedRecords: normalizeInt(usage.usedRecords, 0), userId: id };
}

async function setUsageByUserId(env, userId, usedRecords) {
  const id = normalizeUserId(userId);
  if (!id) return;
  await putState(env, `db:usage:user:${id}`, {
    userId: id,
    usedRecords: Math.max(0, Number(usedRecords) || 0),
    updatedAt: Date.now()
  });
}

// ─── Stripe helpers ───────────────────────────────────────

function getStripeSecret(env) {
  return String(env.STRIPE_SECRET_KEY || '').trim();
}

function getStripePublishableKey(env) {
  return String(env.STRIPE_PUBLISHABLE_KEY || '').trim();
}

function getStripeProductName(env) {
  return String(env.STRIPE_PRODUCT_NAME || '数据库同步飞书多维表格').trim();
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

async function stripeApiRequest(env, path, bodyParams) {
  const secret = getStripeSecret(env);
  if (!secret) throw new Error('未配置 STRIPE_SECRET_KEY');
  const response = await fetch(`https://api.stripe.com${path}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${secret}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams(bodyParams).toString()
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload?.error?.message || `Stripe 请求失败: ${response.status}`);
  return payload;
}

async function stripeApiGet(env, path, params = {}) {
  const secret = getStripeSecret(env);
  if (!secret) throw new Error('未配置 STRIPE_SECRET_KEY');
  const query = new URLSearchParams(params).toString();
  const response = await fetch(`https://api.stripe.com${path}${query ? `?${query}` : ''}`, {
    method: 'GET',
    headers: { Authorization: `Bearer ${secret}` }
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload?.error?.message || `Stripe 请求失败: ${response.status}`);
  return payload;
}

async function resolveStripePrice(env, productName) {
  const configuredPriceId = String(env.STRIPE_PRICE_ID || '').trim();
  if (configuredPriceId) {
    const price = await stripeApiGet(env, `/v1/prices/${configuredPriceId}`);
    return { priceId: configuredPriceId, mode: price?.recurring ? 'subscription' : 'payment' };
  }
  const targetName = String(productName || '').trim();
  const products = await stripeApiGet(env, '/v1/products', { active: 'true', limit: '100' });
  const matchedProduct = asArray(products?.data).find((p) => String(p?.name || '').trim() === targetName);
  if (!matchedProduct?.id) throw new Error(`未在 Stripe 中找到产品：${targetName}`);
  const prices = await stripeApiGet(env, '/v1/prices', { active: 'true', limit: '100', product: String(matchedProduct.id) });
  const matchedPrice = asArray(prices?.data).find((p) => String(p?.id || '').startsWith('price_'));
  if (!matchedPrice?.id) throw new Error(`未在 Stripe 中找到价格：${targetName}`);
  return { priceId: String(matchedPrice.id), mode: matchedPrice?.recurring ? 'subscription' : 'payment' };
}

// ─── Stripe webhook verification ─────────────────────────

function timingSafeEqual(a, b) {
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i += 1) out |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return out === 0;
}

function parseStripeSignature(header) {
  const output = { t: '', v1: '' };
  for (const part of String(header || '').split(',')) {
    const [k, v] = part.split('=');
    if (k === 't') output.t = v;
    if (k === 'v1') output.v1 = v;
  }
  return output;
}

async function hmacSha256Hex(secret, content) {
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(content));
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

async function verifyStripeWebhook(req, env, rawBody) {
  const secret = String(env.STRIPE_WEBHOOK_SECRET || '').trim();
  if (!secret) throw new Error('未配置 STRIPE_WEBHOOK_SECRET');
  const parsed = parseStripeSignature(req.headers.get('Stripe-Signature'));
  if (!parsed.t || !parsed.v1) throw new Error('缺少 Stripe-Signature');
  const expected = await hmacSha256Hex(secret, `${parsed.t}.${rawBody}`);
  if (!timingSafeEqual(expected, parsed.v1)) throw new Error('Stripe webhook 签名校验失败');
}

// ─── KV-based job persistence ─────────────────────────────

async function loadJobs(env) {
  const jobList = await getState(env, 'db:jobs:list');
  return Array.isArray(jobList) ? jobList : [];
}

async function saveJobs(env, jobIds) {
  await putState(env, 'db:jobs:list', jobIds);
}

async function getJob(env, jobId) {
  return getState(env, `db:job:${jobId}`);
}

async function saveJob(env, job) {
  await putState(env, `db:job:${job.id}`, job);
}

async function removeJob(env, jobId) {
  await deleteState(env, `db:job:${jobId}`);
}

async function getIncrementalState(env, jobId, tableName) {
  return getState(env, `db:sync-state:${jobId}:${tableName}`);
}

async function saveIncrementalState(env, jobId, tableName, state) {
  await putState(env, `db:sync-state:${jobId}:${tableName}`, state);
}

async function deleteIncrementalStates(env, jobId, tableNames) {
  for (const tableName of tableNames) {
    await deleteState(env, `db:sync-state:${jobId}:${tableName}`);
  }
}

// ─── cron expression matcher ──────────────────────────────

function cronFieldMatches(fieldExpr, value, minVal, maxVal) {
  // Handle */n
  if (fieldExpr.startsWith('*/')) {
    const step = parseInt(fieldExpr.slice(2), 10);
    return step > 0 && value % step === 0;
  }
  // Handle *
  if (fieldExpr === '*') return true;
  // Handle comma-separated values
  const parts = fieldExpr.split(',');
  for (const part of parts) {
    // Handle range: a-b
    if (part.includes('-')) {
      const [a, b] = part.split('-').map(Number);
      if (value >= a && value <= b) return true;
    } else {
      if (parseInt(part, 10) === value) return true;
    }
  }
  return false;
}

function cronMatches(expression, date) {
  const parts = expression.trim().split(/\s+/);
  if (parts.length < 5) return false;
  const [minuteExpr, hourExpr, dayExpr, monthExpr, dowExpr] = parts;
  const minute = date.getUTCMinutes();
  const hour = date.getUTCHours();
  const day = date.getUTCDate();
  const month = date.getUTCMonth() + 1;
  const dow = date.getUTCDay(); // 0=Sun

  return (
    cronFieldMatches(minuteExpr, minute, 0, 59) &&
    cronFieldMatches(hourExpr, hour, 0, 23) &&
    cronFieldMatches(dayExpr, day, 1, 31) &&
    cronFieldMatches(monthExpr, month, 1, 12) &&
    cronFieldMatches(dowExpr, dow, 0, 6)
  );
}

// ─── job execution ────────────────────────────────────────

async function executeJob(env, job) {
  const driver = getDriver(job.dbType);
  const dbConfig = job.dbConfig;
  const results = [];

  for (const tableName of job.selectedTables) {
    const incrementalOpts = {};
    if (job.syncMode === 'incremental' && job.incrementalConfig?.[tableName]) {
      const pk = job.incrementalConfig[tableName].primaryKey;
      const state = await getIncrementalState(env, job.id, tableName);
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
      job.rowLimit || 500,
      incrementalOpts.primaryKey ? incrementalOpts : undefined
    );

    const result = await syncTableToBitable(
      job.bitableToken,
      job.bitableAppToken,
      tableData,
      job.tablePrefix || '',
      job.syncMode || 'full'
    );

    // Update incremental state
    if (job.syncMode === 'incremental' && job.incrementalConfig?.[tableName]) {
      const pk = job.incrementalConfig[tableName].primaryKey;
      if (pk && tableData.rows.length > 0) {
        const lastRow = tableData.rows[tableData.rows.length - 1];
        await saveIncrementalState(env, job.id, tableName, {
          primaryKey: pk,
          lastValue: lastRow[pk]
        });
      }
    }

    results.push(result);
  }

  return results;
}

// ─── route handlers: quota / payment (preserved) ──────────

async function handleDbQuota(req, env) {
  const url = new URL(req.url);
  const userId = normalizeUserId(url.searchParams.get('userId'));
  if (!userId) {
    return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 userId 参数' });
  }
  const entitlement = await getEntitlementByUserId(env, userId);
  const paid = resolveEntitlementActive(entitlement);
  if (paid) {
    return jsonResponse(req, env, 200, { paid: true, used: 0, remaining: Infinity, total: Infinity });
  }
  const usage = await getUsageByUserId(env, userId);
  const used = usage.usedRecords;
  const remaining = Math.max(0, FREE_DB_QUOTA - used);
  return jsonResponse(req, env, 200, { paid: false, used, remaining, total: FREE_DB_QUOTA });
}

async function handleDbUsage(req, env) {
  const body = (await req.json().catch(() => ({}))) || {};
  const userId = normalizeUserId(body?.userId);
  const count = normalizeInt(body?.count, 0, 0, 500);
  if (!userId) {
    return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 userId 参数' });
  }
  const usage = await getUsageByUserId(env, userId);
  const newUsed = (usage.usedRecords || 0) + count;
  await setUsageByUserId(env, userId, newUsed);
  return jsonResponse(req, env, 200, { status: 'ok', used: newUsed });
}

async function handleGetEntitlement(req, env) {
  const url = new URL(req.url);
  const userId = normalizeUserId(url.searchParams.get('userId'));
  const email = normalizeEmail(url.searchParams.get('email'));
  if (!userId && !email) {
    return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 userId 参数' });
  }
  const entitlement = userId ? await getEntitlementByUserId(env, userId) : await getEntitlementByEmail(env, email);
  const usage = await getUsageByUserId(env, userId || '');
  const active = resolveEntitlementActive(entitlement);
  const remainingFree = Math.max(0, FREE_DB_QUOTA - usage.usedRecords);
  return jsonResponse(req, env, 200, {
    status: 'ok',
    entitlement: entitlement ? { ...entitlement, active } : { active: false, userId: userId || '', email: email || '', expiresAt: 0 },
    freeQuota: { total: FREE_DB_QUOTA, used: usage.usedRecords, remaining: remainingFree }
  });
}

async function handleCreateCheckoutSession(req, env) {
  const body = (await req.json().catch(() => ({}))) || {};
  const successUrl = String(body?.successUrl || '').trim();
  const cancelUrl = String(body?.cancelUrl || '').trim();
  const customerEmail = String(body?.customerEmail || '').trim();
  const userId = String(body?.userId || '').trim();
  const productName = String(body?.productName || getStripeProductName(env)).trim();
  if (!successUrl || !cancelUrl) {
    return jsonResponse(req, env, 400, { status: 'failed', message: 'successUrl 和 cancelUrl 不能为空' });
  }
  try {
    const resolved = await resolveStripePrice(env, productName);
    const mode = String(env.STRIPE_CHECKOUT_MODE || resolved.mode).trim();
    const sessionParams = {
      mode,
      success_url: successUrl,
      cancel_url: cancelUrl,
      'line_items[0][price]': resolved.priceId,
      'line_items[0][quantity]': '1',
      allow_promotion_codes: 'true',
      'metadata[user_id]': userId,
      'metadata[product_name]': productName
    };
    if (customerEmail) {
      sessionParams.customer_email = customerEmail;
      sessionParams['metadata[customer_email]'] = customerEmail;
    }
    const session = await stripeApiRequest(env, '/v1/checkout/sessions', sessionParams);
    return jsonResponse(req, env, 200, { status: 'ok', url: session?.url, sessionId: session?.id, publishableKey: getStripePublishableKey(env) });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '创建结算会话失败' });
  }
}

async function handleStripeWebhook(req, env) {
  const rawBody = await req.text();
  let event;
  try {
    await verifyStripeWebhook(req, env, rawBody);
    event = JSON.parse(rawBody);
  } catch (error) {
    return jsonResponse(req, env, 400, { status: 'failed', message: error?.message || 'webhook 校验失败' });
  }

  const eventId = String(event?.id || '');
  if (eventId) {
    const seen = await getState(env, `db:webhook:event:${eventId}`);
    if (seen) return jsonResponse(req, env, 200, { received: true, dedup: true });
    await putState(env, `db:webhook:event:${eventId}`, { receivedAt: Date.now() });
  }

  try {
    const type = String(event?.type || '');
    const object = event?.data?.object || {};
    const expectedProduct = getStripeProductName(env);

    if (type === 'checkout.session.completed') {
      const eventProduct = String(object?.metadata?.product_name || '').trim();
      if (eventProduct !== expectedProduct) {
        return jsonResponse(req, env, 200, { received: true, skipped: true, reason: 'product_mismatch' });
      }
      const email = String(object?.customer_details?.email || object?.customer_email || object?.metadata?.customer_email || '').trim();
      const userId = String(object?.metadata?.user_id || '').trim();
      const customerId = String(object?.customer || '').trim();
      const mode = String(object?.mode || '');
      if (customerId && (email || userId)) {
        await putState(env, `db:customer:${customerId}`, { email, userId, productName: expectedProduct, updatedAt: Date.now() });
      }
      const expiresAt = mode === 'subscription' ? 0 : Date.now() + YEAR_SECONDS * 1000;
      await markEntitlementActive(env, { email, userId }, 'checkout.session.completed', expiresAt);
    }
    if (type === 'customer.subscription.updated' || type === 'customer.subscription.created') {
      const customerId = String(object?.customer || '').trim();
      const customerState = customerId ? await getState(env, `db:customer:${customerId}`) : null;
      if (!customerState || customerState.productName !== expectedProduct) {
        return jsonResponse(req, env, 200, { received: true, skipped: true, reason: customerState ? 'product_mismatch' : 'unknown_customer' });
      }
      const status = String(object?.status || '');
      const email = String(customerState.email || '').trim();
      const userId = String(customerState.userId || '').trim();
      const periodEnd = Number(object?.current_period_end || 0);
      const expiresAt = periodEnd > 0 ? periodEnd * 1000 : Date.now() + YEAR_SECONDS * 1000;
      if (status === 'active' || status === 'trialing') {
        await markEntitlementActive(env, { email, userId }, type, expiresAt);
      } else if (status) {
        await markEntitlementInactive(env, { email, userId }, `${type}:${status}`);
      }
    }
    if (type === 'customer.subscription.deleted') {
      const customerId = String(object?.customer || '').trim();
      const customerState = customerId ? await getState(env, `db:customer:${customerId}`) : null;
      if (!customerState || customerState.productName !== expectedProduct) {
        return jsonResponse(req, env, 200, { received: true, skipped: true, reason: customerState ? 'product_mismatch' : 'unknown_customer' });
      }
      const email = String(customerState.email || '').trim();
      const userId = String(customerState.userId || '').trim();
      await markEntitlementInactive(env, { email, userId }, type);
    }
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || 'webhook 处理失败' });
  }

  return jsonResponse(req, env, 200, { received: true });
}

// ─── route handlers: database connect / sync ──────────────

async function handleDbConnect(req, env) {
  try {
    const body = await req.json();
    const config = normalizeDbConfig(body);
    validateDbConfig(config);
    const driver = getDriver(config.dbType);
    const tables = await driver.listTables(config);
    return jsonResponse(req, env, 200, { status: 'ok', tables });
  } catch (error) {
    return jsonResponse(req, env, 400, { status: 'failed', message: error?.message || '连接数据库失败' });
  }
}

async function handleDbSync(req, env) {
  try {
    const body = await req.json();
    const config = normalizeDbConfig(body);
    validateDbConfig(config);
    const selectedTables = normalizeSelectedTables(body?.selectedTables);
    const rowLimit = normalizeRowLimit(body?.rowLimit);
    const syncMode = String(body?.syncMode || 'full');
    const incrementalConfig = body?.incrementalConfig || {};

    if (!selectedTables.length) {
      return jsonResponse(req, env, 400, { status: 'failed', message: '至少选择一张数据表' });
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

    return jsonResponse(req, env, 200, { status: 'ok', tables: tablePayload });
  } catch (error) {
    return jsonResponse(req, env, 400, { status: 'failed', message: error?.message || '同步数据库数据失败' });
  }
}

// ─── route handlers: scheduled jobs CRUD ──────────────────

async function handleListJobs(req, env) {
  try {
    const jobIds = await loadJobs(env);
    const jobs = [];
    for (const jobId of jobIds) {
      const job = await getJob(env, jobId);
      if (job) {
        jobs.push({
          id: job.id,
          name: job.name,
          dbType: job.dbType,
          database: job.dbConfig?.database || '',
          selectedTables: job.selectedTables,
          syncMode: job.syncMode,
          cronExpression: job.cronExpression,
          createdAt: job.createdAt,
          lastRun: job.lastRun,
          lastStatus: job.lastStatus,
          lastError: job.lastError
        });
      }
    }
    return jsonResponse(req, env, 200, { status: 'ok', jobs });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '获取任务列表失败' });
  }
}

async function handleCreateJob(req, env) {
  try {
    const body = await req.json();
    if (!body.bitableToken) throw new Error('请输入多维表格授权码');
    if (!body.bitableAppToken) throw new Error('请输入多维表格 App Token');
    if (!body.selectedTables?.length) throw new Error('请选择要同步的数据表');
    const dbConfig = normalizeDbConfig(body);
    validateDbConfig(dbConfig);

    const job = {
      id: generateId(),
      name: body.name || '定时同步任务',
      dbType: dbConfig.dbType,
      dbConfig,
      selectedTables: normalizeSelectedTables(body.selectedTables),
      rowLimit: normalizeRowLimit(body.rowLimit),
      tablePrefix: String(body.tablePrefix || ''),
      syncMode: String(body.syncMode || 'full'),
      incrementalConfig: body.incrementalConfig || {},
      bitableToken: body.bitableToken,
      bitableAppToken: body.bitableAppToken,
      cronExpression: body.cronExpression || '0 * * * *',
      createdAt: new Date().toISOString(),
      lastRun: null,
      lastStatus: null,
      lastError: null
    };

    await saveJob(env, job);
    const jobIds = await loadJobs(env);
    jobIds.push(job.id);
    await saveJobs(env, jobIds);

    return jsonResponse(req, env, 200, {
      status: 'ok',
      job: { id: job.id, name: job.name, cronExpression: job.cronExpression }
    });
  } catch (error) {
    return jsonResponse(req, env, 400, { status: 'failed', message: error?.message || '创建任务失败' });
  }
}

async function handleDeleteJob(req, env, jobId) {
  try {
    const job = await getJob(env, jobId);
    if (!job) {
      return jsonResponse(req, env, 404, { status: 'failed', message: '任务不存在' });
    }

    // Remove from job list
    const jobIds = await loadJobs(env);
    const filtered = jobIds.filter((id) => id !== jobId);
    await saveJobs(env, filtered);

    // Delete incremental states
    await deleteIncrementalStates(env, jobId, job.selectedTables || []);

    // Remove job data
    await removeJob(env, jobId);

    return jsonResponse(req, env, 200, { status: 'ok' });
  } catch (error) {
    return jsonResponse(req, env, 400, { status: 'failed', message: error?.message || '删除任务失败' });
  }
}

async function handleRunJob(req, env, jobId) {
  try {
    const job = await getJob(env, jobId);
    if (!job) {
      return jsonResponse(req, env, 404, { status: 'failed', message: '任务不存在' });
    }

    const results = await executeJob(env, job);

    // Update job status
    job.lastRun = new Date().toISOString();
    job.lastStatus = 'success';
    job.lastError = null;
    await saveJob(env, job);

    return jsonResponse(req, env, 200, { status: 'ok', results });
  } catch (error) {
    // Update job status with error
    const job = await getJob(env, jobId);
    if (job) {
      job.lastRun = new Date().toISOString();
      job.lastStatus = 'failed';
      job.lastError = error?.message || '执行失败';
      await saveJob(env, job);
    }
    return jsonResponse(req, env, 400, { status: 'failed', message: error?.message || '执行任务失败' });
  }
}

// ─── Alipay helpers ──────────────────────────────────────

function getAlipayConfig(env) {
  return {
    appId: String(env.ALIPAY_APP_ID || '').trim(),
    privateKey: String(env.ALIPAY_PRIVATE_KEY || '').trim(),
    alipayPublicKey: String(env.ALIPAY_PUBLIC_KEY || '').trim(),
    gateway: String(env.ALIPAY_GATEWAY || 'https://openapi.alipay.com/gateway.do').trim(),
    productName: String(env.ALIPAY_PRODUCT_NAME || '数据库同步飞书多维表格').trim(),
    totalAmount: String(env.ALIPAY_TOTAL_AMOUNT || '4000.00').trim(),
    notifyUrl: String(env.ALIPAY_NOTIFY_URL || '').trim()
  };
}

function formatAlipayDate(date) {
  const y = date.getFullYear();
  const M = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  const h = String(date.getHours()).padStart(2, '0');
  const m = String(date.getMinutes()).padStart(2, '0');
  const s = String(date.getSeconds()).padStart(2, '0');
  return `${y}-${M}-${d} ${h}:${m}:${s}`;
}

function generateOutTradeNo() {
  return `dbsync_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Convert PEM-formatted PKCS#8 private key to CryptoKey for RSA-SHA256 signing.
 */
async function importAlipayPrivateKey(pemKey) {
  const lines = pemKey
    .replace(/-----BEGIN (?:RSA )?PRIVATE KEY-----/g, '')
    .replace(/-----END (?:RSA )?PRIVATE KEY-----/g, '')
    .replace(/\s+/g, '');
  const binaryDer = Uint8Array.from(atob(lines), (c) => c.charCodeAt(0));
  return crypto.subtle.importKey(
    'pkcs8',
    binaryDer.buffer,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false,
    ['sign']
  );
}

/**
 * RSA2 (SHA256WithRSA) sign for Alipay.
 */
async function rsaSign(content, privateKeyPem) {
  const key = await importAlipayPrivateKey(privateKeyPem);
  const encoded = new TextEncoder().encode(content);
  const signature = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', key, encoded);
  // Convert to base64
  return btoa(String.fromCharCode(...new Uint8Array(signature)));
}

/**
 * Build sorted query string for Alipay signature.
 */
function buildAlipaySignContent(params) {
  return Object.keys(params)
    .filter((k) => params[k] !== undefined && params[k] !== '' && k !== 'sign')
    .sort()
    .map((k) => `${k}=${params[k]}`)
    .join('&');
}

/**
 * Alipay precreate (face-to-face payment) — generates QR code URL.
 */
async function handleAlipayPrecreate(req, env) {
  try {
    const body = (await req.json().catch(() => ({}))) || {};
    const userId = String(body?.userId || '').trim();
    if (!userId) {
      return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 userId' });
    }

    const config = getAlipayConfig(env);
    if (!config.appId || !config.privateKey) {
      return jsonResponse(req, env, 500, { status: 'failed', message: '未配置 ALIPAY_APP_ID 或 ALIPAY_PRIVATE_KEY' });
    }

    const outTradeNo = generateOutTradeNo();
    const now = new Date();
    const timestamp = formatAlipayDate(now);

    const bizContent = JSON.stringify({
      out_trade_no: outTradeNo,
      total_amount: config.totalAmount,
      subject: config.productName,
      timeout_express: '5m'
    });

    const params = {
      app_id: config.appId,
      method: 'alipay.trade.precreate',
      format: 'JSON',
      charset: 'utf-8',
      sign_type: 'RSA2',
      timestamp,
      version: '1.0',
      biz_content: bizContent
    };
    if (config.notifyUrl) {
      params.notify_url = config.notifyUrl;
    }

    const signContent = buildAlipaySignContent(params);
    const sign = await rsaSign(signContent, config.privateKey);
    params.sign = sign;

    // Call Alipay gateway
    const formBody = new URLSearchParams(params).toString();
    const response = await fetch(config.gateway, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8' },
      body: formBody
    });
    const result = await response.json();
    const precreateResponse = result?.alipay_trade_precreate_response;

    if (!precreateResponse || precreateResponse.code !== '10000') {
      const msg = precreateResponse?.sub_msg || precreateResponse?.msg || '预下单失败';
      return jsonResponse(req, env, 400, { status: 'failed', message: msg });
    }

    // Store order info in KV for later verification
    await putState(env, `db:alipay:order:${outTradeNo}`, {
      outTradeNo,
      userId,
      totalAmount: config.totalAmount,
      tradeStatus: 'WAIT_BUYER_PAY',
      createdAt: Date.now()
    });

    return jsonResponse(req, env, 200, {
      status: 'ok',
      qrCode: precreateResponse.qr_code,
      outTradeNo,
      totalAmount: config.totalAmount
    });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '创建支付宝订单失败' });
  }
}

/**
 * Query Alipay trade status.
 */
async function handleAlipayQuery(req, env) {
  try {
    const url = new URL(req.url);
    const outTradeNo = String(url.searchParams.get('outTradeNo') || '').trim();
    if (!outTradeNo) {
      return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 outTradeNo' });
    }

    const config = getAlipayConfig(env);
    if (!config.appId || !config.privateKey) {
      return jsonResponse(req, env, 500, { status: 'failed', message: '未配置支付宝参数' });
    }

    const timestamp = formatAlipayDate(new Date());
    const bizContent = JSON.stringify({ out_trade_no: outTradeNo });

    const params = {
      app_id: config.appId,
      method: 'alipay.trade.query',
      format: 'JSON',
      charset: 'utf-8',
      sign_type: 'RSA2',
      timestamp,
      version: '1.0',
      biz_content: bizContent
    };

    const signContent = buildAlipaySignContent(params);
    const sign = await rsaSign(signContent, config.privateKey);
    params.sign = sign;

    const formBody = new URLSearchParams(params).toString();
    const response = await fetch(config.gateway, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8' },
      body: formBody
    });
    const result = await response.json();
    const queryResponse = result?.alipay_trade_query_response;

    if (!queryResponse || queryResponse.code !== '10000') {
      // Trade not found yet — still waiting
      return jsonResponse(req, env, 200, {
        status: 'ok',
        tradeStatus: 'WAIT_BUYER_PAY',
        outTradeNo
      });
    }

    const tradeStatus = String(queryResponse.trade_status || 'WAIT_BUYER_PAY');

    // If paid, activate entitlement
    if (tradeStatus === 'TRADE_SUCCESS' || tradeStatus === 'TRADE_FINISHED') {
      const order = await getState(env, `db:alipay:order:${outTradeNo}`);
      if (order && order.userId) {
        await markEntitlementActive(env, { userId: order.userId }, 'alipay', Date.now() + YEAR_SECONDS * 1000);
        // Update order status
        order.tradeStatus = tradeStatus;
        order.paidAt = Date.now();
        await putState(env, `db:alipay:order:${outTradeNo}`, order);
      }
    }

    return jsonResponse(req, env, 200, {
      status: 'ok',
      tradeStatus,
      outTradeNo
    });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '查询订单失败' });
  }
}

/**
 * Alipay async notify handler.
 */
async function handleAlipayNotify(req, env) {
  try {
    const text = await req.text();
    const params = Object.fromEntries(new URLSearchParams(text));
    const outTradeNo = String(params.out_trade_no || '');
    const tradeStatus = String(params.trade_status || '');

    if ((tradeStatus === 'TRADE_SUCCESS' || tradeStatus === 'TRADE_FINISHED') && outTradeNo) {
      const order = await getState(env, `db:alipay:order:${outTradeNo}`);
      if (order && order.userId) {
        await markEntitlementActive(env, { userId: order.userId }, 'alipay_notify', Date.now() + YEAR_SECONDS * 1000);
        order.tradeStatus = tradeStatus;
        order.paidAt = Date.now();
        await putState(env, `db:alipay:order:${outTradeNo}`, order);
      }
    }

    // Alipay expects "success" as plain text response
    return new Response('success', {
      status: 200,
      headers: { 'Content-Type': 'text/plain', ...corsHeaders(req, env) }
    });
  } catch (error) {
    return new Response('fail', { status: 200, headers: { 'Content-Type': 'text/plain' } });
  }
}

// ─── router ───────────────────────────────────────────────

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const { pathname } = url;

    if (req.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(req, env) });
    }

    // Health check
    if (req.method === 'GET' && pathname === '/healthz') {
      return jsonResponse(req, env, 200, { ok: true, now: Date.now() });
    }

    // ── Database connect / sync ──
    if (req.method === 'POST' && pathname === '/api/db/connect') {
      return handleDbConnect(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/db/sync') {
      return handleDbSync(req, env);
    }

    // ── MySQL compat aliases ──
    if (req.method === 'POST' && pathname === '/api/mysql/connect') {
      return handleDbConnect(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/mysql/sync') {
      return handleDbSync(req, env);
    }

    // ── Quota / Usage ──
    if (req.method === 'GET' && pathname === '/api/db/quota') {
      return handleDbQuota(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/db/usage') {
      return handleDbUsage(req, env);
    }

    // ── Scheduled Jobs ──
    if (req.method === 'GET' && pathname === '/api/jobs') {
      return handleListJobs(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/jobs') {
      return handleCreateJob(req, env);
    }

    // Match /api/jobs/:jobId and /api/jobs/:jobId/run
    const jobMatch = pathname.match(/^\/api\/jobs\/([^/]+)$/);
    const jobRunMatch = pathname.match(/^\/api\/jobs\/([^/]+)\/run$/);

    if (req.method === 'DELETE' && jobMatch) {
      return handleDeleteJob(req, env, decodeURIComponent(jobMatch[1]));
    }
    if (req.method === 'POST' && jobRunMatch) {
      return handleRunJob(req, env, decodeURIComponent(jobRunMatch[1]));
    }

    // ── Stripe ──
    if (req.method === 'GET' && pathname === '/api/stripe/entitlement') {
      return handleGetEntitlement(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/stripe/create-checkout-session') {
      return handleCreateCheckoutSession(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/stripe/webhook') {
      return handleStripeWebhook(req, env);
    }

    // ── Alipay ──
    if (req.method === 'POST' && pathname === '/api/alipay/precreate') {
      return handleAlipayPrecreate(req, env);
    }
    if (req.method === 'GET' && pathname === '/api/alipay/query') {
      return handleAlipayQuery(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/alipay/notify') {
      return handleAlipayNotify(req, env);
    }

    return jsonResponse(req, env, 404, { status: 'failed', message: 'Not Found' });
  },

  // ─── Cron Trigger handler ───────────────────────────────
  async scheduled(event, env, ctx) {
    const now = new Date(event.scheduledTime);
    console.log(`[cron] Triggered at ${now.toISOString()}`);

    const jobIds = await loadJobs(env);
    if (!jobIds.length) {
      console.log('[cron] No jobs configured');
      return;
    }

    for (const jobId of jobIds) {
      const job = await getJob(env, jobId);
      if (!job) continue;

      // Check if cron expression matches current time
      if (!cronMatches(job.cronExpression, now)) continue;

      console.log(`[cron] Executing job: ${jobId} (${job.name})`);
      try {
        const results = await executeJob(env, job);
        job.lastRun = now.toISOString();
        job.lastStatus = 'success';
        job.lastError = null;
        await saveJob(env, job);
        console.log(`[cron] Job ${jobId} completed:`, results.map((r) => `${r.tableName}(${r.rowCount}行)`).join(', '));
      } catch (error) {
        job.lastRun = now.toISOString();
        job.lastStatus = 'failed';
        job.lastError = error?.message || '执行失败';
        await saveJob(env, job);
        console.error(`[cron] Job ${jobId} failed:`, error?.message);
      }
    }
  }
};
