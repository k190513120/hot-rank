/**
 * Cloudflare Worker — 数据库同步插件
 *
 * 独立于 weread-sync-service，拥有自己的 KV namespace 和 Stripe 产品。
 * 实际 MySQL 同步走 Node 服务端，本 Worker 只负责：
 *   - 配额检查 (GET  /api/db/quota)
 *   - 用量上报 (POST /api/db/usage)
 *   - Stripe 支付（checkout、webhook、entitlement）
 */

const FREE_DB_QUOTA = 3;
const YEAR_SECONDS = 365 * 24 * 60 * 60;

// ─── helpers ──────────────────────────────────────────────

function corsHeaders(req, env) {
  const requestOrigin = req.headers.get('Origin') || '*';
  const allowOrigin = env.ALLOWED_ORIGIN || requestOrigin || '*';
  return {
    'Access-Control-Allow-Origin': allowOrigin,
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
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

// ─── KV state ─────────────────────────────────────────────

async function putState(env, key, value) {
  if (!env.DB_STATE) return;
  await env.DB_STATE.put(key, JSON.stringify(value), { expirationTtl: 60 * 60 * 24 * 90 });
}

async function getState(env, key) {
  if (!env.DB_STATE) return null;
  const value = await env.DB_STATE.get(key);
  if (!value) return null;
  return JSON.parse(value);
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

// ─── route handlers ───────────────────────────────────────

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
    const session = await stripeApiRequest(env, '/v1/checkout/sessions', {
      mode,
      success_url: successUrl,
      cancel_url: cancelUrl,
      'line_items[0][price]': resolved.priceId,
      'line_items[0][quantity]': '1',
      allow_promotion_codes: 'true',
      customer_email: customerEmail,
      'metadata[user_id]': userId,
      'metadata[customer_email]': customerEmail,
      'metadata[product_name]': productName
    });
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
      if (eventProduct && eventProduct !== expectedProduct) {
        return jsonResponse(req, env, 200, { received: true, skipped: true, reason: 'product_mismatch' });
      }
      const email = String(object?.customer_details?.email || object?.customer_email || object?.metadata?.customer_email || '').trim();
      const userId = String(object?.metadata?.user_id || '').trim();
      const customerId = String(object?.customer || '').trim();
      const mode = String(object?.mode || '');
      if (customerId && (email || userId)) {
        await putState(env, `db:customer:${customerId}`, { email, userId, updatedAt: Date.now() });
      }
      const expiresAt = mode === 'subscription' ? 0 : Date.now() + YEAR_SECONDS * 1000;
      await markEntitlementActive(env, { email, userId }, 'checkout.session.completed', expiresAt);
    }
    if (type === 'customer.subscription.updated' || type === 'customer.subscription.created') {
      const customerId = String(object?.customer || '').trim();
      const customerState = customerId ? await getState(env, `db:customer:${customerId}`) : null;
      if (!customerState) {
        return jsonResponse(req, env, 200, { received: true, skipped: true, reason: 'unknown_customer' });
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
      if (!customerState) {
        return jsonResponse(req, env, 200, { received: true, skipped: true, reason: 'unknown_customer' });
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

// ─── router ───────────────────────────────────────────────

export default {
  async fetch(req, env) {
    const { pathname } = new URL(req.url);
    if (req.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(req, env) });
    }
    if (req.method === 'GET' && pathname === '/healthz') {
      return jsonResponse(req, env, 200, { ok: true, now: Date.now() });
    }
    if (req.method === 'GET' && pathname === '/api/db/quota') {
      return handleDbQuota(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/db/usage') {
      return handleDbUsage(req, env);
    }
    if (req.method === 'GET' && pathname === '/api/stripe/entitlement') {
      return handleGetEntitlement(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/stripe/create-checkout-session') {
      return handleCreateCheckoutSession(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/stripe/webhook') {
      return handleStripeWebhook(req, env);
    }
    return jsonResponse(req, env, 404, { status: 'failed', message: 'Not Found' });
  }
};
