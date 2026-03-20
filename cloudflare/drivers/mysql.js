/**
 * MySQL driver for Cloudflare Workers — raw wire protocol over cloudflare:sockets.
 * No eval()/new Function() — fully compatible with V8 isolate sandbox.
 */
import { connect } from 'cloudflare:sockets';

// ─── MySQL protocol constants ────────────────────────────────

const CLIENT_LONG_PASSWORD     = 0x00000001;
const CLIENT_FOUND_ROWS        = 0x00000002;
const CLIENT_LONG_FLAG         = 0x00000004;
const CLIENT_CONNECT_WITH_DB   = 0x00000008;
const CLIENT_PROTOCOL_41       = 0x00000200;
const CLIENT_SECURE_CONNECTION = 0x00008000;
const CLIENT_PLUGIN_AUTH       = 0x00080000;

const COM_QUERY = 0x03;
const COM_QUIT  = 0x01;

const CHARSET_UTF8MB4 = 45;

// ─── Binary helpers ──────────────────────────────────────────

class BufferReader {
  constructor(buf) { this.buf = buf; this.pos = 0; }

  u8()  { return this.buf[this.pos++]; }
  u16() { const v = this.buf[this.pos] | (this.buf[this.pos + 1] << 8); this.pos += 2; return v; }
  u32() { const v = (this.buf[this.pos]) | (this.buf[this.pos+1]<<8) | (this.buf[this.pos+2]<<16) | (this.buf[this.pos+3]<<24); this.pos += 4; return v >>> 0; }

  bytes(n) { const s = this.buf.subarray(this.pos, this.pos + n); this.pos += n; return s; }

  nullStr() {
    const start = this.pos;
    while (this.pos < this.buf.length && this.buf[this.pos] !== 0) this.pos++;
    const str = new TextDecoder().decode(this.buf.subarray(start, this.pos));
    this.pos++; // skip null
    return str;
  }

  lenEncInt() {
    const first = this.u8();
    if (first < 0xfb) return first;
    if (first === 0xfb) return null; // NULL
    if (first === 0xfc) return this.u16();
    if (first === 0xfd) { const v = this.buf[this.pos]|(this.buf[this.pos+1]<<8)|(this.buf[this.pos+2]<<16); this.pos+=3; return v; }
    if (first === 0xfe) { const lo = this.u32(); const hi = this.u32(); return hi * 0x100000000 + lo; }
    return 0;
  }

  lenEncStr() {
    const len = this.lenEncInt();
    if (len === null) return null;
    const s = new TextDecoder().decode(this.buf.subarray(this.pos, this.pos + len));
    this.pos += len;
    return s;
  }

  remaining() { return this.buf.length - this.pos; }
}

// ─── MySQL Connection ────────────────────────────────────────

class MySQLConnection {
  constructor() {
    this.socket = null;
    this.reader = null;
    this.writer = null;
    this.seqId = 0;
    this.pending = new Uint8Array(0);
  }

  async open(host, port, username, password, database) {
    this.socket = connect({ hostname: host, port });
    this.reader = this.socket.readable.getReader();
    this.writer = this.socket.writable.getWriter();

    const handshake = await this._readHandshake();
    await this._authenticate(handshake, username, password, database);
  }

  // ── packet I/O ──

  _append(data) {
    const c = new Uint8Array(this.pending.length + data.length);
    c.set(this.pending, 0);
    c.set(data, this.pending.length);
    this.pending = c;
  }

  async _ensure(n) {
    while (this.pending.length < n) {
      const { value, done } = await this.reader.read();
      if (done) throw new Error('MySQL connection closed unexpectedly');
      this._append(value);
    }
  }

  async _readPacket() {
    await this._ensure(4);
    const len = this.pending[0] | (this.pending[1] << 8) | (this.pending[2] << 16);
    this.seqId = this.pending[3] + 1;
    await this._ensure(4 + len);
    const payload = this.pending.slice(4, 4 + len);
    this.pending = this.pending.slice(4 + len);
    return payload;
  }

  async _writePacket(payload) {
    const pkt = new Uint8Array(4 + payload.length);
    pkt[0] = payload.length & 0xff;
    pkt[1] = (payload.length >> 8) & 0xff;
    pkt[2] = (payload.length >> 16) & 0xff;
    pkt[3] = this.seqId++;
    pkt.set(payload, 4);
    await this.writer.write(pkt);
  }

  // ── handshake ──

  async _readHandshake() {
    const data = await this._readPacket();
    const r = new BufferReader(data);

    const protocolVersion = r.u8();
    const serverVersion = r.nullStr();
    const connId = r.u32();
    const scramble1 = r.bytes(8);
    r.u8(); // filler
    const capLow = r.u16();
    const charset = r.u8();
    const statusFlags = r.u16();
    const capHigh = r.u16();
    const caps = capLow | (capHigh << 16);

    let authDataLen = 0;
    if (caps & CLIENT_PLUGIN_AUTH) authDataLen = r.u8();
    else r.u8();

    r.bytes(10); // reserved

    let scramble2 = new Uint8Array(0);
    if (caps & CLIENT_SECURE_CONNECTION) {
      const len = Math.max(13, authDataLen - 8);
      scramble2 = r.bytes(len);
      // strip trailing null
      if (scramble2[scramble2.length - 1] === 0) scramble2 = scramble2.subarray(0, -1);
    }

    let pluginName = 'mysql_native_password';
    if ((caps & CLIENT_PLUGIN_AUTH) && r.remaining() > 0) pluginName = r.nullStr();

    const scramble = new Uint8Array(scramble1.length + scramble2.length);
    scramble.set(scramble1, 0);
    scramble.set(scramble2, scramble1.length);

    return { caps, scramble, pluginName };
  }

  // ── authentication ──

  async _authenticate(hs, username, password, database) {
    const authData = await this._computeAuth(password, hs.scramble, hs.pluginName);

    const flags = CLIENT_LONG_PASSWORD | CLIENT_FOUND_ROWS | CLIENT_LONG_FLAG |
                  CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION |
                  CLIENT_PLUGIN_AUTH;

    const enc = new TextEncoder();
    const userBuf = enc.encode(username);
    const dbBuf = enc.encode(database);
    const pluginBuf = enc.encode(hs.pluginName);

    const size = 4 + 4 + 1 + 23 + userBuf.length + 1 + 1 + authData.length + dbBuf.length + 1 + pluginBuf.length + 1;
    const buf = new Uint8Array(size);
    let o = 0;

    // capability flags
    buf[o++] = flags & 0xff; buf[o++] = (flags>>8) & 0xff;
    buf[o++] = (flags>>16) & 0xff; buf[o++] = (flags>>24) & 0xff;
    // max packet size (16MB)
    buf[o++] = 0; buf[o++] = 0; buf[o++] = 0; buf[o++] = 1;
    // charset
    buf[o++] = CHARSET_UTF8MB4;
    // reserved 23 bytes
    o += 23;
    // username null-terminated
    buf.set(userBuf, o); o += userBuf.length; buf[o++] = 0;
    // auth data length-prefixed
    buf[o++] = authData.length;
    buf.set(authData, o); o += authData.length;
    // database null-terminated
    buf.set(dbBuf, o); o += dbBuf.length; buf[o++] = 0;
    // plugin name null-terminated
    buf.set(pluginBuf, o); o += pluginBuf.length; buf[o++] = 0;

    await this._writePacket(buf.subarray(0, o));

    const resp = await this._readPacket();

    // 0xff = ERR
    if (resp[0] === 0xff) {
      this._throwErr(resp);
    }

    // 0xfe = auth switch request
    if (resp[0] === 0xfe) {
      const sr = new BufferReader(resp);
      sr.u8(); // 0xfe
      const newPlugin = sr.nullStr();
      let newScramble = sr.bytes(sr.remaining());
      if (newScramble.length > 0 && newScramble[newScramble.length - 1] === 0)
        newScramble = newScramble.subarray(0, -1);

      const newAuth = await this._computeAuth(password, newScramble, newPlugin);
      await this._writePacket(newAuth);

      const resp2 = await this._readPacket();
      if (resp2[0] === 0xff) this._throwErr(resp2);

      // caching_sha2_password may respond with 0x01 0x03 (fast auth ok)
      // or 0x01 0x04 (need full auth — requires SSL, not supported here)
      if (resp2[0] === 0x01 && resp2.length >= 2) {
        if (resp2[1] === 0x04) {
          throw new Error('MySQL: caching_sha2_password full authentication requires SSL/TLS. Use mysql_native_password or enable SSL.');
        }
        // 0x03 = fast auth success, read final OK
        if (resp2[1] === 0x03) {
          const okPkt = await this._readPacket();
          if (okPkt[0] === 0xff) this._throwErr(okPkt);
        }
      }
    }
    // 0x00 = OK, authenticated
  }

  async _computeAuth(password, scramble, plugin) {
    if (!password) return new Uint8Array(0);
    if (plugin === 'caching_sha2_password') return this._cachingSha2(password, scramble);
    return this._nativePassword(password, scramble);
  }

  async _nativePassword(password, scramble) {
    // SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))
    const enc = new TextEncoder();
    const pwBuf = enc.encode(password);
    const sha1pw   = new Uint8Array(await crypto.subtle.digest('SHA-1', pwBuf));
    const sha1sha1 = new Uint8Array(await crypto.subtle.digest('SHA-1', sha1pw));
    const combined = new Uint8Array(scramble.length + sha1sha1.length);
    combined.set(scramble, 0);
    combined.set(sha1sha1, scramble.length);
    const sha1scr = new Uint8Array(await crypto.subtle.digest('SHA-1', combined));
    const out = new Uint8Array(20);
    for (let i = 0; i < 20; i++) out[i] = sha1pw[i] ^ sha1scr[i];
    return out;
  }

  async _cachingSha2(password, scramble) {
    // SHA256(password) XOR SHA256(SHA256(SHA256(password)) + scramble)
    const enc = new TextEncoder();
    const pwBuf = enc.encode(password);
    const sha256pw   = new Uint8Array(await crypto.subtle.digest('SHA-256', pwBuf));
    const sha256x2   = new Uint8Array(await crypto.subtle.digest('SHA-256', sha256pw));
    const combined   = new Uint8Array(sha256x2.length + scramble.length);
    combined.set(sha256x2, 0);
    combined.set(scramble, sha256x2.length);
    const sha256scr  = new Uint8Array(await crypto.subtle.digest('SHA-256', combined));
    const out = new Uint8Array(32);
    for (let i = 0; i < 32; i++) out[i] = sha256pw[i] ^ sha256scr[i];
    return out;
  }

  _throwErr(pkt) {
    const r = new BufferReader(pkt);
    r.u8(); // 0xff
    const code = r.u16();
    if (r.remaining() > 0 && pkt[r.pos] === 0x23) r.bytes(6); // SQL state
    const msg = new TextDecoder().decode(pkt.subarray(r.pos));
    throw new Error(`MySQL error ${code}: ${msg}`);
  }

  // ── query ──

  async query(sql) {
    this.seqId = 0;
    const enc = new TextEncoder();
    const sqlBuf = enc.encode(sql);
    const payload = new Uint8Array(1 + sqlBuf.length);
    payload[0] = COM_QUERY;
    payload.set(sqlBuf, 1);
    await this._writePacket(payload);

    const first = await this._readPacket();

    // ERR packet
    if (first[0] === 0xff) this._throwErr(first);

    // OK packet (INSERT/UPDATE/DELETE etc.)
    if (first[0] === 0x00) return { columns: [], rows: [] };

    // Result set — first byte is column count (lenenc int)
    const cr = new BufferReader(first);
    const colCount = cr.lenEncInt();

    // Column definitions
    const columns = [];
    for (let i = 0; i < colCount; i++) {
      const colPkt = await this._readPacket();
      const c = new BufferReader(colPkt);
      c.lenEncStr(); // catalog
      c.lenEncStr(); // schema
      c.lenEncStr(); // table (virtual)
      c.lenEncStr(); // org_table
      const name = c.lenEncStr(); // column name
      c.lenEncStr(); // org_name
      c.bytes(1);    // filler 0x0c
      const charset = c.u16();
      const colLen = c.u32();
      const colType = c.u8();
      columns.push({ name, type: colType, charset, colLen });
    }

    // EOF after columns
    await this._readPacket();

    // Rows
    const rows = [];
    while (true) {
      const rowPkt = await this._readPacket();
      // EOF
      if (rowPkt[0] === 0xfe && rowPkt.length < 9) break;
      // ERR
      if (rowPkt[0] === 0xff) break;

      const rr = new BufferReader(rowPkt);
      const row = {};
      for (let i = 0; i < colCount; i++) {
        if (rr.buf[rr.pos] === 0xfb) {
          rr.u8();
          row[columns[i].name] = null;
        } else {
          row[columns[i].name] = rr.lenEncStr();
        }
      }
      rows.push(row);
    }

    return { columns, rows };
  }

  async close() {
    try { this.seqId = 0; await this._writePacket(new Uint8Array([COM_QUIT])); } catch (_) {}
    try { this.reader.releaseLock(); } catch (_) {}
    try { this.writer.releaseLock(); } catch (_) {}
    try { this.socket.close(); } catch (_) {}
  }
}

// ─── Escape identifier (backtick-quoted) ─────────────────────

function escapeId(name) {
  return '`' + String(name).replace(/`/g, '``') + '`';
}

// ─── Escape string value for SQL ─────────────────────────────

function escapeString(val) {
  if (val === null || val === undefined) return 'NULL';
  return "'" + String(val).replace(/[\0\x08\x09\x1a\n\r'\\]/g, (ch) => {
    switch (ch) {
      case '\0': return '\\0';
      case '\x08': return '\\b';
      case '\x09': return '\\t';
      case '\x1a': return '\\Z';
      case '\n': return '\\n';
      case '\r': return '\\r';
      case "'": return "\\'";
      case '\\': return '\\\\';
      default: return ch;
    }
  }) + "'";
}

// ─── Helper: open connection, run fn, close ──────────────────

async function withConnection(config, fn) {
  const conn = new MySQLConnection();
  await conn.open(
    config.host,
    config.port || 3306,
    config.username,
    config.password,
    config.database
  );
  try {
    return await fn(conn);
  } finally {
    await conn.close();
  }
}

// ─── Exported API (same interface as before) ─────────────────

export async function listTables(config) {
  return withConnection(config, async (conn) => {
    const result = await conn.query(
      `SELECT TABLE_NAME AS tableName, COALESCE(TABLE_ROWS, 0) AS estimatedRows
       FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA = ${escapeString(config.database)}
       ORDER BY TABLE_NAME ASC`
    );
    return result.rows.map((r) => ({
      tableName: String(r.tableName || ''),
      estimatedRows: Number(r.estimatedRows || 0)
    }));
  });
}

export async function fetchTableData(config, tableName, rowLimit, incrementalOpts) {
  return withConnection(config, async (conn) => {
    const colResult = await conn.query(
      `SELECT COLUMN_NAME AS columnName, DATA_TYPE AS dataType
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = ${escapeString(config.database)} AND TABLE_NAME = ${escapeString(tableName)}
       ORDER BY ORDINAL_POSITION ASC`
    );
    const columnList = colResult.rows.map((c) => ({
      name: String(c.columnName || ''),
      dataType: String(c.dataType || 'text')
    }));
    if (!columnList.length) return { tableName, columns: [], rows: [] };

    let query = `SELECT * FROM ${escapeId(tableName)}`;
    if (incrementalOpts?.primaryKey && incrementalOpts?.lastValue != null) {
      query += ` WHERE ${escapeId(incrementalOpts.primaryKey)} > ${escapeString(String(incrementalOpts.lastValue))}`;
      query += ` ORDER BY ${escapeId(incrementalOpts.primaryKey)} ASC`;
    }
    query += ` LIMIT ${Math.max(1, Math.min(10000, Number(rowLimit) || 500))}`;

    const dataResult = await conn.query(query);
    return { tableName, columns: columnList, rows: dataResult.rows };
  });
}
