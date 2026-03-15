const WEREAD_URL = 'https://weread.qq.com/';
const WEREAD_NOTEBOOKS_URL = 'https://weread.qq.com/api/user/notebook';
const WEREAD_BOOKMARK_LIST_URL = 'https://weread.qq.com/web/book/bookmarklist';
const WEREAD_REVIEW_LIST_URL = 'https://weread.qq.com/web/review/list';
const WEREAD_CHAPTER_INFOS_URL = 'https://weread.qq.com/web/book/chapterInfos';
const FREE_RECORD_QUOTA = 10;
const YEAR_SECONDS = 365 * 24 * 60 * 60;

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function pickString(source, keys) {
  for (const key of keys) {
    const value = source?.[key];
    if (typeof value === 'string' && value.trim()) return value.trim();
    if (typeof value === 'number') return String(value);
  }
  return '';
}

function toTimestamp(value) {
  if (typeof value === 'number') {
    if (value <= 0) return undefined;
    return value > 1e12 ? value : value * 1000;
  }
  if (typeof value === 'string' && value.trim()) {
    const t = new Date(value).getTime();
    if (!Number.isNaN(t)) return t;
  }
  return undefined;
}

function normalizeHighlight(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const tags = Array.isArray(raw.tags) ? raw.tags.map((v) => String(v)).join('、') : pickString(raw, ['tags', 'tag']);
  const result = {
    bookTitle: pickString(raw, ['bookTitle', 'book_name', 'bookName', 'title']),
    author: pickString(raw, ['author', 'bookAuthor']),
    chapter: pickString(raw, ['chapter', 'chapterTitle', 'chapter_name']),
    highlightText: pickString(raw, ['highlightText', 'markText', 'text', 'highlight_content']),
    noteText: pickString(raw, ['noteText', 'reviewContent', 'note', 'comment']),
    tags,
    highlightId: pickString(raw, ['highlightId', 'markId', 'bookmarkId']),
    bookId: pickString(raw, ['bookId', 'book_id']),
    highlightedAt: toTimestamp(raw.highlightedAt ?? raw.createTime ?? raw.markTime),
    updatedAt: toTimestamp(raw.updatedAt ?? raw.updateTime)
  };
  if (!result.bookTitle && !result.highlightText && !result.noteText) return null;
  return result;
}

function normalizeInt(value, fallback, min = 1, max = 2000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

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
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      ...corsHeaders(req, env)
    }
  });
}

function getStripeSecret(env) {
  return String(env.STRIPE_SECRET_KEY || '').trim();
}

function getStripePublishableKey(env) {
  return String(env.STRIPE_PUBLISHABLE_KEY || '').trim();
}

function getStripeProductName(env) {
  return String(env.STRIPE_PRODUCT_NAME || '微信读书同步多维表格').trim();
}

async function stripeApiRequest(env, path, bodyParams) {
  const secret = getStripeSecret(env);
  if (!secret) {
    throw new Error('未配置 STRIPE_SECRET_KEY');
  }
  const response = await fetch(`https://api.stripe.com${path}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${secret}`,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: new URLSearchParams(bodyParams).toString()
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload?.error?.message || `Stripe 请求失败: ${response.status}`;
    throw new Error(message);
  }
  return payload;
}

async function stripeApiGet(env, path, params = {}) {
  const secret = getStripeSecret(env);
  if (!secret) {
    throw new Error('未配置 STRIPE_SECRET_KEY');
  }
  const query = new URLSearchParams(params).toString();
  const response = await fetch(`https://api.stripe.com${path}${query ? `?${query}` : ''}`, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${secret}`
    }
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload?.error?.message || `Stripe 请求失败: ${response.status}`;
    throw new Error(message);
  }
  return payload;
}

async function resolveStripePrice(env, productName) {
  const configuredPriceId = String(env.STRIPE_PRICE_ID || '').trim();
  if (configuredPriceId) {
    const price = await stripeApiGet(env, `/v1/prices/${configuredPriceId}`);
    const mode = price?.recurring ? 'subscription' : 'payment';
    return { priceId: configuredPriceId, mode };
  }
  const targetName = String(productName || '').trim();
  const products = await stripeApiGet(env, '/v1/products', { active: 'true', limit: '100' });
  const matchedProduct = asArray(products?.data).find((item) => String(item?.name || '').trim() === targetName);
  if (!matchedProduct?.id) {
    throw new Error(`未在 Stripe 中找到产品，产品名：${targetName}`);
  }
  const prices = await stripeApiGet(env, '/v1/prices', {
    active: 'true',
    limit: '100',
    product: String(matchedProduct.id)
  });
  const matchedPrice = asArray(prices?.data).find((item) => String(item?.id || '').startsWith('price_'));
  if (!matchedPrice?.id) {
    throw new Error(`未在 Stripe 中找到产品价格，产品名：${targetName}`);
  }
  const mode = matchedPrice?.recurring ? 'subscription' : 'payment';
  return { priceId: String(matchedPrice.id), mode };
}

async function putStripeState(env, key, value) {
  if (!env.STRIPE_STATE) return;
  await env.STRIPE_STATE.put(key, JSON.stringify(value), { expirationTtl: 60 * 60 * 24 * 90 });
}

async function getStripeState(env, key) {
  if (!env.STRIPE_STATE) return null;
  const value = await env.STRIPE_STATE.get(key);
  if (!value) return null;
  return JSON.parse(value);
}

function normalizeEmail(input) {
  return String(input || '').trim().toLowerCase();
}

function normalizeUserId(input) {
  return String(input || '').trim();
}

function resolveEntitlementActive(entitlement) {
  if (!entitlement || !entitlement.active) return false;
  const expiresAt = Number(entitlement.expiresAt || 0);
  if (!expiresAt) return true;
  return Date.now() < expiresAt;
}

async function getEntitlementByEmail(env, email) {
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail) return null;
  return getStripeState(env, `stripe:entitlement:email:${normalizedEmail}`);
}

async function getEntitlementByUserId(env, userId) {
  const normalizedUserId = normalizeUserId(userId);
  if (!normalizedUserId) return null;
  return getStripeState(env, `stripe:entitlement:user:${normalizedUserId}`);
}

async function getUsageByEmail(env, email) {
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail) return { usedRecords: 0, email: normalizedEmail };
  const usage = await getStripeState(env, `stripe:usage:email:${normalizedEmail}`);
  if (!usage) return { usedRecords: 0, email: normalizedEmail };
  return {
    usedRecords: normalizeInt(usage.usedRecords, 0, 0, 1000000),
    email: normalizedEmail
  };
}

async function getUsageByUserId(env, userId) {
  const normalizedUserId = normalizeUserId(userId);
  if (!normalizedUserId) return { usedRecords: 0, userId: normalizedUserId };
  const usage = await getStripeState(env, `stripe:usage:user:${normalizedUserId}`);
  if (!usage) return { usedRecords: 0, userId: normalizedUserId };
  return {
    usedRecords: normalizeInt(usage.usedRecords, 0, 0, 1000000),
    userId: normalizedUserId
  };
}

async function setUsageByEmail(env, email, usedRecords) {
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail) return;
  await putStripeState(env, `stripe:usage:email:${normalizedEmail}`, {
    email: normalizedEmail,
    usedRecords: Math.max(0, Number(usedRecords) || 0),
    updatedAt: Date.now()
  });
}

async function setUsageByUserId(env, userId, usedRecords) {
  const normalizedUserId = normalizeUserId(userId);
  if (!normalizedUserId) return;
  await putStripeState(env, `stripe:usage:user:${normalizedUserId}`, {
    userId: normalizedUserId,
    usedRecords: Math.max(0, Number(usedRecords) || 0),
    updatedAt: Date.now()
  });
}

function timingSafeEqual(a, b) {
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i += 1) out |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return out === 0;
}

function parseStripeSignature(signatureHeader) {
  const parts = String(signatureHeader || '').split(',');
  const output = { t: '', v1: '' };
  for (const part of parts) {
    const [k, v] = part.split('=');
    if (k === 't') output.t = v;
    if (k === 'v1') output.v1 = v;
  }
  return output;
}

async function hmacSha256Hex(secret, content) {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const signature = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(content));
  return [...new Uint8Array(signature)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

async function verifyStripeWebhook(req, env, rawBody) {
  const secret = String(env.STRIPE_WEBHOOK_SECRET || '').trim();
  if (!secret) {
    throw new Error('未配置 STRIPE_WEBHOOK_SECRET');
  }
  const signatureHeader = req.headers.get('Stripe-Signature');
  const parsed = parseStripeSignature(signatureHeader);
  if (!parsed.t || !parsed.v1) {
    throw new Error('缺少 Stripe-Signature');
  }
  const signedPayload = `${parsed.t}.${rawBody}`;
  const expected = await hmacSha256Hex(secret, signedPayload);
  if (!timingSafeEqual(expected, parsed.v1)) {
    throw new Error('Stripe webhook 签名校验失败');
  }
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
    const resolvedPrice = await resolveStripePrice(env, productName);
    const mode = String(env.STRIPE_CHECKOUT_MODE || resolvedPrice.mode).trim();
    const session = await stripeApiRequest(env, '/v1/checkout/sessions', {
      mode,
      success_url: successUrl,
      cancel_url: cancelUrl,
      'line_items[0][price]': resolvedPrice.priceId,
      'line_items[0][quantity]': '1',
      allow_promotion_codes: 'true',
      customer_email: customerEmail,
      'metadata[user_id]': userId,
      'metadata[customer_email]': customerEmail,
      'metadata[product_name]': productName
    });
    return jsonResponse(req, env, 200, {
      status: 'ok',
      url: session?.url,
      sessionId: session?.id,
      publishableKey: getStripePublishableKey(env)
    });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '创建结算会话失败' });
  }
}

async function createCheckoutSessionUrl(env, identity, req, metadata = {}) {
  const productName = getStripeProductName(env);
  const resolvedPrice = await resolveStripePrice(env, productName);
  const requestUrl = new URL(req.url);
  const appBaseUrl = String(env.APP_BASE_URL || `${requestUrl.protocol}//${requestUrl.host}`).trim();
  const successUrl = String(env.PAYMENT_SUCCESS_URL || `${appBaseUrl}/payment-success`).trim();
  const cancelUrl = String(env.PAYMENT_CANCEL_URL || `${appBaseUrl}/payment-cancel`).trim();
  const userId = normalizeUserId(identity?.userId);
  const customerEmail = normalizeEmail(identity?.customerEmail);
  const session = await stripeApiRequest(env, '/v1/checkout/sessions', {
    mode: String(env.STRIPE_CHECKOUT_MODE || resolvedPrice.mode).trim(),
    success_url: successUrl,
    cancel_url: cancelUrl,
    'line_items[0][price]': resolvedPrice.priceId,
    'line_items[0][quantity]': '1',
    allow_promotion_codes: 'true',
    ...(customerEmail ? { customer_email: customerEmail } : {}),
    ...(customerEmail ? { 'metadata[customer_email]': customerEmail } : {}),
    ...(userId ? { 'metadata[user_id]': userId } : {}),
    'metadata[source]': 'sync-quota',
    'metadata[product_name]': productName,
    ...(metadata?.userId ? { 'metadata[user_id]': String(metadata.userId) } : {})
  });
  return String(session?.url || '');
}

async function markEntitlementActive(env, identity, source, expiresAt) {
  const normalizedEmail = normalizeEmail(identity?.email);
  const normalizedUserId = normalizeUserId(identity?.userId);
  if (!normalizedEmail && !normalizedUserId) return;
  const resolvedExpiresAt = Number(expiresAt || 0) || Date.now() + YEAR_SECONDS * 1000;
  const payload = {
    active: true,
    email: normalizedEmail || '',
    userId: normalizedUserId || '',
    source,
    expiresAt: resolvedExpiresAt,
    updatedAt: Date.now()
  };
  if (normalizedEmail) {
    await putStripeState(env, `stripe:entitlement:email:${normalizedEmail}`, payload);
  }
  if (normalizedUserId) {
    await putStripeState(env, `stripe:entitlement:user:${normalizedUserId}`, payload);
  }
}

async function markEntitlementInactive(env, identity, source) {
  const normalizedEmail = normalizeEmail(identity?.email);
  const normalizedUserId = normalizeUserId(identity?.userId);
  if (!normalizedEmail && !normalizedUserId) return;
  const payload = {
    active: false,
    email: normalizedEmail || '',
    userId: normalizedUserId || '',
    source,
    updatedAt: Date.now()
  };
  if (normalizedEmail) {
    await putStripeState(env, `stripe:entitlement:email:${normalizedEmail}`, payload);
  }
  if (normalizedUserId) {
    await putStripeState(env, `stripe:entitlement:user:${normalizedUserId}`, payload);
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
    const seen = await getStripeState(env, `stripe:webhook:event:${eventId}`);
    if (seen) {
      return jsonResponse(req, env, 200, { received: true, dedup: true });
    }
    await putStripeState(env, `stripe:webhook:event:${eventId}`, { receivedAt: Date.now() });
  }

  try {
    const type = String(event?.type || '');
    const object = event?.data?.object || {};
    if (type === 'checkout.session.completed') {
      const email = String(object?.customer_details?.email || object?.customer_email || object?.metadata?.customer_email || '').trim();
      const userId = String(object?.metadata?.user_id || '').trim();
      const customerId = String(object?.customer || '').trim();
      const mode = String(object?.mode || '');
      if (customerId && (email || userId)) {
        await putStripeState(env, `stripe:customer:${customerId}`, {
          email,
          userId,
          updatedAt: Date.now()
        });
      }
      const expiresAt = mode === 'subscription' ? 0 : Date.now() + YEAR_SECONDS * 1000;
      await markEntitlementActive(env, { email, userId }, 'checkout.session.completed', expiresAt);
    }
    if (type === 'customer.subscription.updated' || type === 'customer.subscription.created') {
      const customerId = String(object?.customer || '').trim();
      const status = String(object?.status || '');
      const customerState = customerId ? await getStripeState(env, `stripe:customer:${customerId}`) : null;
      const email = String(customerState?.email || '').trim();
      const userId = String(customerState?.userId || '').trim();
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
      const customerState = customerId ? await getStripeState(env, `stripe:customer:${customerId}`) : null;
      const email = String(customerState?.email || '').trim();
      const userId = String(customerState?.userId || '').trim();
      await markEntitlementInactive(env, { email, userId }, type);
    }
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || 'webhook 处理失败' });
  }

  return jsonResponse(req, env, 200, { received: true });
}

async function handleGetEntitlement(req, env) {
  const url = new URL(req.url);
  const userId = normalizeUserId(url.searchParams.get('userId'));
  const email = normalizeEmail(url.searchParams.get('email'));
  if (!userId && !email) {
    return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 userId 参数' });
  }
  const entitlement = userId
    ? await getEntitlementByUserId(env, userId)
    : await getEntitlementByEmail(env, email);
  const usage = userId
    ? await getUsageByUserId(env, userId)
    : await getUsageByEmail(env, email);
  const active = resolveEntitlementActive(entitlement);
  const remainingFreeRecords = Math.max(0, FREE_RECORD_QUOTA - usage.usedRecords);
  return jsonResponse(req, env, 200, {
    status: 'ok',
    entitlement: entitlement
      ? { ...entitlement, active }
      : { active: false, userId: userId || '', email: email || '', expiresAt: 0 },
    freeQuota: {
      total: FREE_RECORD_QUOTA,
      used: usage.usedRecords,
      remaining: remainingFreeRecords
    }
  });
}

async function checkSyncPermission(body, env) {
  const userId = normalizeUserId(body?.userId);
  if (!userId) {
    return { ok: false, message: '无法识别当前用户，请在多维表格内打开插件重试' };
  }
  const entitlement = await getEntitlementByUserId(env, userId);
  if (resolveEntitlementActive(entitlement)) {
    return { ok: true, userId, paid: true };
  }
  const usage = await getUsageByUserId(env, userId);
  const remaining = Math.max(0, FREE_RECORD_QUOTA - usage.usedRecords);
  if (remaining <= 0) {
    return { ok: false, userId, paymentRequired: true, message: '免费 10 条额度已用完，请先完成支付' };
  }
  return { ok: true, userId, paid: false, remainingFreeRecords: remaining, usedRecords: usage.usedRecords };
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `请求失败: ${response.status}`);
  }
  return response.json();
}

function toCookieHeaderFromCookieCloudDomain(list) {
  const pairs = asArray(list)
    .map((item) => {
      const name = String(item?.name || '').trim();
      const value = String(item?.value || '').trim();
      if (!name) return '';
      return `${name}=${value}`;
    })
    .filter(Boolean);
  return pairs.join('; ');
}

async function getWereadCookieFromCookieCloud(env) {
  const cookieCloudUrl = String(env.CC_URL || '').trim().replace(/\/+$/, '');
  const cookieCloudId = String(env.CC_ID || '').trim();
  const cookieCloudPassword = String(env.CC_PASSWORD || '').trim();
  if (!cookieCloudUrl || !cookieCloudId || !cookieCloudPassword) return '';

  const payload = await fetchJson(`${cookieCloudUrl}/get/${encodeURIComponent(cookieCloudId)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ password: cookieCloudPassword })
  });

  const cookieData = payload?.cookie_data || payload?.data?.cookie_data || {};
  const wereadCookies = cookieData?.['weread.qq.com'];
  return toCookieHeaderFromCookieCloudDomain(wereadCookies);
}

async function wereadRequestJson(url, options, wereadCookie) {
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      Referer: WEREAD_URL,
      Origin: 'https://weread.qq.com',
      'User-Agent': 'Mozilla/5.0',
      Cookie: wereadCookie,
      ...(options?.headers || {})
    }
  });

  const contentType = String(response.headers.get('content-type') || '').toLowerCase();
  const bodyText = await response.text();

  if (!response.ok) {
    const snippet = bodyText.replace(/\s+/g, ' ').slice(0, 120);
    throw new Error(`微信读书接口失败: ${response.status} ${snippet}`);
  }
  if (!contentType.includes('application/json')) {
    const snippet = bodyText.replace(/\s+/g, ' ').slice(0, 120);
    throw new Error(`微信读书返回非JSON内容，通常是Cookie失效或被风控拦截: ${snippet}`);
  }
  try {
    return JSON.parse(bodyText);
  } catch (_) {
    const snippet = bodyText.replace(/\s+/g, ' ').slice(0, 120);
    throw new Error(`微信读书JSON解析失败: ${snippet}`);
  }
}

async function fetchChapterMap(bookId, wereadCookie) {
  const payload = await wereadRequestJson(
    WEREAD_CHAPTER_INFOS_URL,
    {
      method: 'POST',
      body: JSON.stringify({
        bookIds: [bookId],
        syncKeys: [0],
        teenmode: 0
      })
    },
    wereadCookie
  );
  const list = asArray(payload?.data);
  if (!list.length) return {};
  const updated = asArray(list[0]?.updated);
  const chapterMap = {};
  for (const item of updated) {
    const chapterUid = item?.chapterUid;
    if (chapterUid !== undefined && chapterUid !== null) {
      chapterMap[String(chapterUid)] = String(item?.title || '');
    }
  }
  return chapterMap;
}

async function fetchBookmarks(bookId, wereadCookie) {
  const url = `${WEREAD_BOOKMARK_LIST_URL}?bookId=${encodeURIComponent(bookId)}`;
  const payload = await wereadRequestJson(url, { method: 'GET' }, wereadCookie);
  return asArray(payload?.updated);
}

async function fetchReviews(bookId, wereadCookie) {
  const params = new URLSearchParams({
    bookId: String(bookId),
    listType: '4',
    maxIdx: '0',
    count: '50',
    listMode: '2',
    syncKey: '0'
  });
  const payload = await wereadRequestJson(`${WEREAD_REVIEW_LIST_URL}?${params.toString()}`, { method: 'GET' }, wereadCookie);
  return asArray(payload?.reviews)
    .map((item) => item?.review || item || null)
    .filter(Boolean)
    .map((review) => ({
      markText: String(review?.content || review?.abstract || ''),
      abstract: String(review?.abstract || ''),
      reviewId: String(review?.reviewId || ''),
      createTime: review?.createTime,
      chapterUid: Number(review?.type) === 4 ? 1000000 : review?.chapterUid
    }))
    .filter((x) => x.markText);
}

async function fetchNotebooks(wereadCookie, maxBooksLimit) {
  const payload = await wereadRequestJson(WEREAD_NOTEBOOKS_URL, { method: 'GET' }, wereadCookie);
  const books = asArray(payload?.books).sort((a, b) => Number(b?.sort || 0) - Number(a?.sort || 0));
  return books.slice(0, maxBooksLimit);
}

async function fetchHighlightsByWereadCookie(wereadCookie, maxRecords, maxBooksLimit) {
  if (!wereadCookie) return { highlights: [], books: [] };
  const books = await fetchNotebooks(wereadCookie, maxBooksLimit);
  const highlights = [];
  const booksMeta = [];
  for (const item of books) {
    if (highlights.length >= maxRecords) break;
    const book = item?.book || {};
    const bookId = String(book?.bookId || '');
    if (!bookId) continue;

    const bookTitle = String(book?.title || '');
    const author = String(book?.author || '');
    const tags = asArray(book?.categories).map((x) => String(x?.title || '')).filter(Boolean).join('、');
    booksMeta.push(item);

    let chapterMap = {};
    try {
      chapterMap = await fetchChapterMap(bookId, wereadCookie);
    } catch (_) {
      chapterMap = {};
    }

    try {
      const bookmarks = await fetchBookmarks(bookId, wereadCookie);
      const reviews = await fetchReviews(bookId, wereadCookie);
      for (const row of [...bookmarks, ...reviews]) {
        const chapter = chapterMap[String(row?.chapterUid)] || '';
        const mapped = normalizeHighlight({
          bookTitle,
          author,
          chapter,
          highlightText: row?.markText || row?.text || row?.highlightText,
          noteText: row?.abstract || '',
          tags,
          highlightId: row?.reviewId || row?.bookmarkId || row?.markId || '',
          bookId,
          highlightedAt: row?.createTime || row?.markTime,
          updatedAt: row?.updateTime || row?.createTime
        });
        if (mapped) highlights.push(mapped);
        if (highlights.length >= maxRecords) break;
      }
    } catch (_) {}
  }
  return {
    highlights: highlights.slice(0, maxRecords),
    books: booksMeta
  };
}

async function resolveCookie(body, env) {
  const directCookie = String(body?.wereadCookie || body?.cookie || '').trim();
  if (directCookie) return directCookie;
  try {
    const ccCookie = await getWereadCookieFromCookieCloud(env);
    if (ccCookie) return ccCookie;
  } catch (_) {}
  return String(env.WEREAD_COOKIE || '').trim();
}

async function handleSync(req, env) {
  const body = (await req.json().catch(() => ({}))) || {};
  const permission = await checkSyncPermission(body, env);
  if (!permission.ok) {
    if (permission.paymentRequired) {
      try {
        const checkoutUrl = await createCheckoutSessionUrl(
          env,
          { userId: permission.userId, customerEmail: body?.customerEmail || body?.email },
          req,
          { userId: permission.userId }
        );
        return jsonResponse(req, env, 200, {
          status: 'payment_required',
          message: permission.message,
          checkoutUrl
        });
      } catch (error) {
        return jsonResponse(req, env, 500, { status: 'failed', message: `创建支付链接失败：${error?.message || 'unknown error'}` });
      }
    }
    return jsonResponse(req, env, 400, { status: 'failed', message: permission.message });
  }
  const maxBooks = normalizeInt(body?.maxBooks, normalizeInt(env.WEREAD_MAX_SYNC_BOOKS, 2000));
  let maxRecords = normalizeInt(body?.maxRecords, normalizeInt(env.WEREAD_MAX_SYNC_RECORDS, 200));
  if (!permission.paid) {
    maxRecords = Math.max(1, Math.min(maxRecords, permission.remainingFreeRecords));
  }
  const strictRealData = String(env.WEREAD_STRICT_REAL_DATA || 'true') === 'true';
  const wereadCookie = await resolveCookie(body, env);

  if (!wereadCookie) {
    return jsonResponse(req, env, 400, { status: 'failed', message: '缺少可用 Cookie，请传 wereadCookie 或配置 CookieCloud/WEREAD_COOKIE' });
  }

  try {
    const syncPayload = await fetchHighlightsByWereadCookie(wereadCookie, maxRecords, maxBooks);
    const highlights = asArray(syncPayload?.highlights);
    const books = asArray(syncPayload?.books);
    if (!highlights.length && strictRealData) {
      return jsonResponse(req, env, 500, { status: 'failed', message: '真实数据抓取结果为空，请检查 Cookie 是否有效' });
    }
    if (!permission.paid) {
      const usedRecords = (permission.usedRecords || 0) + highlights.length;
      await setUsageByUserId(env, permission.userId, usedRecords);
    }
    return jsonResponse(req, env, 200, { status: 'completed', highlights, books });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: `真实数据抓取失败：${error?.message || 'unknown error'}` });
  }
}

export default {
  async fetch(req, env) {
    const { pathname } = new URL(req.url);
    if (req.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(req, env) });
    }
    if (req.method === 'GET' && pathname === '/healthz') {
      return jsonResponse(req, env, 200, { ok: true, now: Date.now() });
    }
    if (req.method === 'POST' && pathname === '/api/weread/sync') {
      return handleSync(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/stripe/create-checkout-session') {
      return handleCreateCheckoutSession(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/stripe/webhook') {
      return handleStripeWebhook(req, env);
    }
    if (req.method === 'GET' && pathname === '/api/stripe/entitlement') {
      return handleGetEntitlement(req, env);
    }
    if (req.method === 'GET' && pathname.startsWith('/api/weread/sync/')) {
      return jsonResponse(req, env, 400, { status: 'failed', message: 'Cloudflare 版本为实时同步，不支持任务轮询接口' });
    }
    return jsonResponse(req, env, 404, { status: 'failed', message: 'Not Found' });
  }
};
