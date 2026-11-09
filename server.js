// Ferry server: groups devices that are on the same network and passes
// connection setup messages between them. Files never touch this server.
// No dependencies. Run:  node server.js   (PORT env var changes the port)

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');
// Only these files are ever served, so server.js and the rest stay private.
const PUBLIC_FILES = new Set(['index.html', 'sw.js', 'manifest.webmanifest', 'icon-192.png', 'icon-512.png', 'icon-maskable-512.png', 'favicon-32.png', 'favicon.ico', 'favicon.svg', 'logo.png', 'apple-touch-icon.png', 'ferry.apk']);
const MAX_PER_ROOM = 50;
const DATA_DIR = process.env.DATA_DIR || __dirname;       // where names.txt lives
const NAMES_FILE = path.join(DATA_DIR, 'names.txt');
const MAX_NAMES = 200000;                                 // devices remembered (login history has no limit)
const LOGIN_GAP = Number(process.env.LOGIN_GAP_MS) || 5 * 60 * 1000;                         // a device that was away this long and comes back counts as a new login
const KEY_GIVEN = !!process.env.ADMIN_KEY;
const ADMIN_KEY = process.env.ADMIN_KEY || crypto.randomBytes(9).toString('base64url');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.webmanifest': 'application/manifest+json',
  '.json': 'application/json',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.apk': 'application/vnd.android.package-archive'
};

const rooms = new Map();  // room key -> Map(id -> peer)
const byId = new Map();   // id -> peer
const PING_MS = 10000;    // server -> device keep-alive event
const STALE_MS = Number(process.env.STALE_MS) || 45000;   // a device that has not checked in for this long is gone (its phone slept, lost signal, etc.)

// ICE servers handed to every device. Set ICE_SERVERS (JSON) to add a TURN server for transfers between different networks.
const DEFAULT_ICE = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302', 'stun:stun.cloudflare.com:3478'] }];
let ICE = DEFAULT_ICE;
try {
  if (process.env.ICE_SERVERS) {
    const v = JSON.parse(process.env.ICE_SERVERS);
    if (Array.isArray(v) && v.length) ICE = v;
  }
} catch (e) { console.error('ICE_SERVERS is not valid JSON. Using the default STUN servers.'); }

/* ---- who is "nearby" ---- */
function clientIp(req) {
  const fwd = req.headers['x-forwarded-for'];
  let ip = fwd ? String(fwd).split(',')[0].trim() : req.socket.remoteAddress || '';
  if (ip.startsWith('::ffff:')) ip = ip.slice(7);
  return ip;
}
function isPrivate(ip) {
  if (ip === '::1' || ip === '127.0.0.1') return true;
  if (/^(10\.|192\.168\.|169\.254\.|127\.)/.test(ip)) return true;
  const m = /^172\.(\d+)\./.exec(ip);
  if (m && +m[1] >= 16 && +m[1] <= 31) return true;
  return /^(fe80|fc|fd)/i.test(ip);
}
function v6prefix(ip) {
  let g;
  if (ip.includes('::')) {
    const [h, t] = ip.split('::');
    const hs = h ? h.split(':') : [];
    const ts = t ? t.split(':') : [];
    g = [...hs, ...Array(Math.max(0, 8 - hs.length - ts.length)).fill('0'), ...ts];
  } else g = ip.split(':');
  return g.slice(0, 4).map((x) => x.toLowerCase().replace(/^0+(?=.)/, '')).join(':');
}
function roomKey(ip, room) {
  if (room) return 'room:' + room.toLowerCase();
  if (isPrivate(ip)) return 'local';                  // everyone on this LAN or this machine
  return 'net:' + (ip.includes(':') ? v6prefix(ip) : ip); // same public address = same Wi-Fi
}

/* ---- helpers ---- */
const clean = (s, n) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, n);
function emit(peer, event, data) {
  try { peer.res.write('event: ' + event + '\ndata: ' + JSON.stringify(data) + '\n\n'); } catch (e) {}
}
// d = a stable hash of the device id, so the app can tell two tabs of the same device apart from two devices.
const pubHash = (did) => (did ? crypto.createHash('sha256').update('ferry:' + did).digest('hex').slice(0, 12) : '');
const pub = (p) => ({ id: p.id, name: p.name, type: p.type, d: p.d, j: p.joined });
function others(peer) {
  const room = rooms.get(peer.key);
  return room ? [...room.values()].filter((p) => p !== peer) : [];
}
// Removes a device exactly once, whatever noticed it was gone (closed stream, missed heartbeat, leave message, newer connection).
function dropPeer(peer, endStream) {
  if (!peer || peer.gone) return;
  peer.gone = true;
  clearInterval(peer.beat);
  if (peer.counted) {
    const left = (liveCount.get(peer.did) || 1) - 1;
    if (left > 0) liveCount.set(peer.did, left); else { liveCount.delete(peer.did); lastEnd.set(peer.did, Date.now()); }
  }
  touch(peer.did);
  byId.delete(peer.id);
  const room = rooms.get(peer.key);
  if (room) {
    room.delete(peer.id);
    room.forEach((p) => emit(p, 'peer-left', { id: peer.id }));
    if (room.size === 0) rooms.delete(peer.key);
  }
  if (endStream) { try { peer.res.end(); } catch (e) {} }
}
function readBody(req, limit = 65536) {
  return new Promise((resolve, reject) => {
    let size = 0; const parts = [];
    req.on('data', (c) => { size += c.length; if (size > limit) { reject(new Error('too large')); req.destroy(); } else parts.push(c); });
    req.on('end', () => { try { resolve(JSON.parse(Buffer.concat(parts).toString() || '{}')); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
}

/* ---- device names list (names.txt) ---- */
// Every device tells the server its name when it connects. The server keeps one line per device.
// Only the owner can read the list, with the ADMIN_KEY. IP addresses are never stored.
const registry = new Map();   // device id -> { name, type, first, last }
const TYPE_LABEL = { iphone: 'iPhone', ipad: 'iPad', android: 'Android', tablet: 'Android tablet', computer: 'Computer' };
const budget = new Map();     // ip -> { n, reset }: limits how many new devices one address can add per hour
const failures = new Map();   // ip -> { n, reset }: wrong key attempts
const stamp = (t) => new Date(t).toISOString().slice(0, 16).replace('T', ' ');
const unstamp = (s) => { const t = Date.parse(s.replace(' ', 'T') + ':00Z'); return Number.isFinite(t) ? t : Date.now(); };
const validId = (s) => /^[a-f0-9]{16,32}$/.test(s || '');

// Turns the browser's user-agent into something readable, e.g. "Chrome 124 on Android 14".
function parseUA(ua) {
  ua = String(ua || '');
  let os = 'Unknown OS', br = 'Unknown browser', m;
  if ((m = /Android ([\d.]+)/.exec(ua))) os = 'Android ' + m[1];
  else if ((m = /(?:iPhone|iPad|iPod).*? OS ([\d_]+)/.exec(ua))) os = 'iOS ' + m[1].replace(/_/g, '.');
  else if (/Windows NT/.test(ua)) os = 'Windows';
  else if (/CrOS/.test(ua)) os = 'ChromeOS';
  else if (/Mac OS X/.test(ua)) os = 'macOS';
  else if (/Linux/.test(ua)) os = 'Linux';
  if ((m = /EdgA?\/(\d+)/.exec(ua))) br = 'Edge ' + m[1];
  else if ((m = /SamsungBrowser\/(\d+)/.exec(ua))) br = 'Samsung Internet ' + m[1];
  else if ((m = /OPR\/(\d+)/.exec(ua))) br = 'Opera ' + m[1];
  else if ((m = /(?:CriOS|Chrome)\/(\d+)/.exec(ua))) br = 'Chrome ' + m[1];
  else if ((m = /(?:FxiOS|Firefox)\/(\d+)/.exec(ua))) br = 'Firefox ' + m[1];
  else if ((m = /Version\/(\d+).*Safari/.exec(ua))) br = 'Safari ' + m[1];
  return br + ' on ' + os;
}
// Ordinary details the browser shares by itself. Nothing is read from the phone beyond this.
function describe(req, q) {
  const model = clean(q.get('model'), 40);
  const scr = /^\d{2,5}x\d{2,5}(@[\d.]{1,4})?$/.test(q.get('scr') || '') ? q.get('scr') : '';
  const lang = /^[A-Za-z0-9-]{2,12}$/.test(q.get('lang') || '') ? q.get('lang') : '';
  const tz = /^[A-Za-z0-9_\/+-]{1,40}$/.test(q.get('tz') || '') ? q.get('tz') : '';
  const mode = q.get('mode') === 'app' ? 'installed app' : q.get('mode') === 'browser' ? 'browser tab' : '';
  return [parseUA(req.headers['user-agent']), model, scr, lang, tz, mode].filter(Boolean).join(' · ').replace(/\|/g, '/').slice(0, 220);
}

function namesText() {
  const lines = ['# Ferry device names (times are UTC)', '# name | device | first seen | last seen | id | ip address | details'];
  [...registry.entries()].sort((a, b) => a[1].first - b[1].first).forEach(([id, r]) => {
    lines.push([r.name, TYPE_LABEL[r.type] || 'Device', stamp(r.first), stamp(r.last), id, r.ip || '-', r.info || '-'].join(' | '));
  });
  return lines.join('\n') + '\n';
}
function loadRegistry() {
  let text = '';
  try { text = fs.readFileSync(NAMES_FILE, 'utf8'); } catch (e) { return; }
  const rev = Object.fromEntries(Object.entries(TYPE_LABEL).map(([k, v]) => [v, k]));
  text.split('\n').forEach((line) => {
    if (!line.trim() || line.startsWith('#')) return;
    const p = line.split(' | ').map((x) => x.trim());
    if (p.length < 5 || !p[0] || !validId(p[4])) return;     // older files have only 5 columns
    const info = p.slice(6).join(' | ');
    registry.set(p[4], {
      name: p[0].slice(0, 24), type: rev[p[1]] || 'computer', first: unstamp(p[2]), last: unstamp(p[3]),
      ip: p[5] && p[5] !== '-' ? p[5] : '', info: info && info !== '-' ? info : ''
    });
  });
}
let saveTimer = null;
function scheduleSave(did) {
  if (did) dirty.add(did);
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    const tmp = NAMES_FILE + '.tmp';
    try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(tmp, namesText()); fs.renameSync(tmp, NAMES_FILE); }
    catch (e) { console.error('Could not write names.txt:', e.message); }
    flushRemote();
  }, 500);
}
function register(did, name, type, since, ip, extra) {
  if (!validId(did)) return;
  const label = String(name || '').replace(/\|/g, '/').trim().slice(0, 24) || 'Device';
  const now = Date.now();
  const known = registry.get(did);
  if (known) {
    known.name = label; known.type = type; known.last = now;
    if (extra) { known.ip = extra.ip; known.info = extra.info; }   // only devices that confirmed the notice
    return scheduleSave(did);
  }
  if (registry.size >= MAX_NAMES) return;
  if (budget.size > 10000) budget.clear();
  const b = budget.get(ip);
  if (b && b.reset > now) { if (b.n >= 30) return; b.n++; } else budget.set(ip, { n: 1, reset: now + 3600000 });
  const first = since > 1.6e12 && since <= now ? since : now;   // a device remembers when it first joined
  registry.set(did, { name: label, type, first, last: now, ip: extra ? extra.ip : '', info: extra ? extra.info : '' });
  scheduleSave(did);
}
function touch(did) { const r = registry.get(did); if (r) { r.last = Date.now(); scheduleSave(did); } }
loadRegistry();

/* ---- permanent storage ---- */
// The device list and the login history are kept for good, in two places so a restart or redeploy never loses them:
//   1. Files in DATA_DIR (names.txt and logins.txt). On a host with a persistent disk, point DATA_DIR at it.
//   2. Optionally a free Upstash Redis database over HTTPS (set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN).
//      Free hosts such as Render's free plan wipe their disk on every restart, so this is what keeps the record there.
const REDIS_URL = String(process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN || '';
// Vercel's Redis storage may instead give one connection string (REDIS_URL or KV_URL, redis:// or rediss://). Both ways work.
const REDIS_TCP = (/^rediss?:\/\//i.test(process.env.REDIS_URL || '') && process.env.REDIS_URL) || (/^rediss?:\/\//i.test(process.env.KV_URL || '') && process.env.KV_URL) || '';
const REMOTE = !!((REDIS_URL && REDIS_TOKEN) || REDIS_TCP);
const LOGINS_FILE = path.join(DATA_DIR, 'logins.txt');
const K_DEVICES = 'ferry:devices', K_LOGINS = 'ferry:logins';
const dirty = new Set();      // device ids changed since the last save to the database
const outbox = [];            // login lines not yet saved to the database
const liveCount = new Map();  // device id -> open connections now
const lastEnd = new Map();    // device id -> when its last connection closed

// Minimal Redis client over a plain connection (no packages needed).
function redisTcp(commands) {
  return new Promise((resolve, reject) => {
    const u = new URL(REDIS_TCP);
    const secure = u.protocol === 'rediss:';
    const port = Number(u.port) || 6379;
    const sock = secure ? require('tls').connect({ host: u.hostname, port, servername: u.hostname }) : require('net').connect({ host: u.hostname, port });
    const enc = (c) => '*' + c.length + '\r\n' + c.map((x) => { const b = Buffer.from(String(x)); return '$' + b.length + '\r\n' + b.toString('latin1') + '\r\n'; }).join('');
    const pre = [];
    if (u.password) pre.push(u.username && u.username !== 'default' ? ['AUTH', decodeURIComponent(u.username), decodeURIComponent(u.password)] : ['AUTH', decodeURIComponent(u.password)]);
    else if (u.username && u.username !== 'default') pre.push(['AUTH', 'default', decodeURIComponent(u.username)]);
    const all = pre.concat(commands);
    let buf = Buffer.alloc(0), pos = 0; const out = []; let done = false;
    const finish = (err, val) => { if (done) return; done = true; clearTimeout(timer); sock.destroy(); err ? reject(err) : resolve(val); };
    const timer = setTimeout(() => finish(new Error('database timed out')), 8000);
    const line = () => { const i = buf.indexOf('\r\n', pos); if (i < 0) return null; const t = buf.toString('utf8', pos, i); pos = i + 2; return t; };
    function parse() {   // returns undefined when incomplete
      const save = pos, l = line();
      if (l === null) return undefined;
      const t = l[0], rest = l.slice(1);
      if (t === '+') return rest;
      if (t === '-') return { error: rest };
      if (t === ':') return Number(rest);
      if (t === '$') {
        const n = Number(rest); if (n < 0) return null;
        if (buf.length < pos + n + 2) { pos = save; return undefined; }
        const v = buf.toString('utf8', pos, pos + n); pos += n + 2; return v;
      }
      if (t === '*') {
        const n = Number(rest); if (n < 0) return null;
        const arr = [];
        for (let i = 0; i < n; i++) { const v = parse(); if (v === undefined) { pos = save; return undefined; } arr.push(v); }
        return arr;
      }
      return { error: 'bad reply' };
    }
    sock.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      for (;;) {
        const mark = pos, v = parse();
        if (v === undefined) { pos = mark; break; }
        out.push(v);
        if (out.length === all.length) {
          const bad = out.find((x) => x && x.error);
          return bad ? finish(new Error(bad.error)) : finish(null, out.slice(pre.length));
        }
      }
    });
    sock.on('error', (e) => finish(e));
    sock.on('close', () => finish(new Error('database closed the connection')));
    sock.on(secure ? 'secureConnect' : 'connect', () => sock.write(all.map(enc).join(''), 'latin1'));
  });
}
async function redis(commands) {
  if (REDIS_TCP && !(REDIS_URL && REDIS_TOKEN)) return redisTcp(commands);
  const r = await fetch(REDIS_URL + '/pipeline', {
    method: 'POST', headers: { Authorization: 'Bearer ' + REDIS_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(commands), signal: AbortSignal.timeout(8000)
  });
  if (!r.ok) throw new Error('database answered ' + r.status);
  const out = await r.json();
  const bad = out.find((x) => x && x.error);
  if (bad) throw new Error(bad.error);
  return out.map((x) => x.result);
}
let flushP = Promise.resolve();
async function doFlush() {
  const ids = [...dirty].slice(0, 200), lines = outbox.slice(0, 200);
  if (!ids.length && !lines.length) return;
  const cmds = [];
  ids.forEach((id) => { const r = registry.get(id); if (r) cmds.push(['HSET', K_DEVICES, id, JSON.stringify(r)]); });
  lines.forEach((l) => cmds.push(['RPUSH', K_LOGINS, l]));
  try {
    if (cmds.length) await redis(cmds);
    ids.forEach((id) => dirty.delete(id));
    outbox.splice(0, lines.length);
  } catch (e) { console.error('Could not save to the database (will retry):', e.message); }
}
// Saves are queued one after another so none is skipped (a serverless host may freeze this process right after a login).
function flushRemote() { if (!REMOTE) return Promise.resolve(); flushP = flushP.then(doFlush); return flushP; }
if (REMOTE) setInterval(flushRemote, 2000).unref();

async function loadRemote(quiet) {
  if (!REMOTE) return;
  const [flat] = await redis([['HGETALL', K_DEVICES]]);
  let n = 0;
  for (let i = 0; i + 1 < (flat || []).length; i += 2) {
    try {
      const r = JSON.parse(flat[i + 1]), mine = registry.get(flat[i]);
      if (validId(flat[i]) && (!mine || (r.last || 0) > mine.last)) { registry.set(flat[i], r); n++; }
    } catch (e) {}
  }
  if (!quiet) console.log('  Database: loaded ' + n + ' device' + (n === 1 ? '' : 's') + ' (' + registry.size + ' known)');
  registry.forEach((r, id) => { if (!flat || !flat.includes(id)) dirty.add(id); });   // push anything only the file knew about
}

// One line per login. A login is a device opening Ferry for the first time or coming back after being away for a few minutes,
// not every quick reconnect.
function recordLogin(peer, extra) {
  if (!validId(peer.did)) return;
  const r = registry.get(peer.did);
  const t = new Date().toISOString().slice(0, 19).replace('T', ' ');
  const line = [t, String(peer.name).replace(/\|/g, '/'), TYPE_LABEL[peer.type] || 'Device', peer.did,
    extra && extra.ip ? extra.ip : '-', extra && extra.info ? extra.info : (r && r.info) || '-'].join(' | ');
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.appendFileSync(LOGINS_FILE, line + '\n'); }
  catch (e) { console.error('Could not write logins.txt:', e.message); }
  if (REMOTE) { outbox.push(line); if (outbox.length > 50000) outbox.shift(); flushRemote(); }
}
const LOGIN_HEAD = ['# Ferry login history (one line per login, newest last, times are UTC)', '# time | name | device | id | ip address | details'];
async function readLogins(all) {
  const LIMIT = 2000;
  if (REMOTE) {
    try {
      const [len] = await redis([['LLEN', K_LOGINS]]);
      if (!all) return (await redis([['LRANGE', K_LOGINS, -LIMIT, -1]]))[0] || [];
      const out = [];
      for (let i = 0; i < len; i += 5000) out.push(...((await redis([['LRANGE', K_LOGINS, i, i + 4999]]))[0] || []));
      return out;
    } catch (e) { console.error('Could not read logins from the database:', e.message); }
  }
  try { const lines = fs.readFileSync(LOGINS_FILE, 'utf8').split('\n').filter(Boolean); return all ? lines : lines.slice(-LIMIT); }
  catch (e) { return []; }
}

// Save everything before the host stops this process (a deploy or restart sends SIGTERM).
let stopping = false;
async function shutdown() {
  if (stopping) return; stopping = true;
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(NAMES_FILE, namesText());
    for (let i = 0; i < 20 && (dirty.size || outbox.length) && REMOTE; i++) await Promise.race([flushRemote(), new Promise((r) => setTimeout(r, 3000))]);
  } catch (e) {}
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

async function handleNames(req, res, url) {
  const ip = clientIp(req), now = Date.now();
  const f = failures.get(ip);
  if (f && f.reset > now && f.n >= 10) return json(res, 429, { error: 'Too many wrong keys. Try again in a few minutes.' });
  const given = crypto.createHash('sha256').update(String(url.searchParams.get('key') || '')).digest();
  const real = crypto.createHash('sha256').update(ADMIN_KEY).digest();
  if (!crypto.timingSafeEqual(given, real)) {
    if (f && f.reset > now) f.n++; else failures.set(ip, { n: 1, reset: now + 600000 });
    return json(res, 403, { error: 'Wrong or missing key.' });
  }
  res.writeHead(200, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Cache-Control': 'no-store',
    'X-Robots-Tag': 'noindex',
    'Content-Disposition': url.searchParams.get('download') ? 'attachment; filename="names.txt"' : 'inline'
  });
  if (REMOTE) { try { await loadRemote(true); } catch (e) {} }
  const logins = await readLogins(!!url.searchParams.get('download'));
  const status = REMOTE ? '# Storage: permanent (database). Nothing here is reset by restarts or redeploys.\n' : '# Storage: NOT permanent. No database is connected, so this list resets when the host restarts. Connect Upstash Redis to keep it forever.\n';
  res.end(status + namesText() + '\n' + LOGIN_HEAD.join('\n') + '\n' + logins.join('\n') + (logins.length ? '\n' : ''));
}

/* ---- routes ---- */
function handleEvents(req, res, url) {
  sweep();   // a host that pauses idle servers cannot run timers, so clear out old devices before listing anyone
  const key = roomKey(clientIp(req), clean(url.searchParams.get('room'), 32));
  const did = clean(url.searchParams.get('did'), 32).toLowerCase();
  const tid = clean(url.searchParams.get('tid'), 24).toLowerCase();
  // A page that reconnects replaces its own older connection instead of showing up twice.
  // This must run before the room is looked up, because dropping the last device of a room deletes the room.
  if (tid && validId(did)) {
    for (const old of [...byId.values()]) if (old.did === did && old.tid === tid) dropPeer(old, true);
  }
  if (!rooms.has(key)) rooms.set(key, new Map());
  const room = rooms.get(key);
  if (room.size >= MAX_PER_ROOM) return json(res, 429, { error: 'room full' });

  try { req.socket.setNoDelay(true); req.socket.setKeepAlive(true, 15000); } catch (e) {}
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });
  res.write('retry: 2000\n\n');

  const peer = {
    id: crypto.randomBytes(12).toString('hex'),
    name: clean(url.searchParams.get('name'), 24) || 'Device',
    type: clean(url.searchParams.get('type'), 10) || 'computer',
    did, tid, d: pubHash(validId(did) ? did : ''),
    joined: Date.now(), seen: Date.now(), hb: false,   // hb turns true once the app starts sending heartbeats
    key, res
  };
  const addr = clientIp(req);
  // The address and device details are kept only when the app says the person confirmed the notice (ack=2).
  const extra = url.searchParams.get('ack') === '2'
    ? { ip: /^[0-9a-fA-F:.]{3,45}$/.test(addr) ? addr : '', info: describe(req, url.searchParams) }
    : undefined;
  const prevSeen = registry.has(peer.did) ? registry.get(peer.did).last : 0;
  register(peer.did, peer.name, peer.type, Number(url.searchParams.get('since')), addr, extra);
  if (validId(peer.did)) {
    const open = liveCount.get(peer.did) || 0;
    if (open === 0 && Date.now() - Math.max(prevSeen, lastEnd.get(peer.did) || 0) > LOGIN_GAP) recordLogin(peer, extra);
    liveCount.set(peer.did, open + 1);
    peer.counted = true;
  }
  room.set(peer.id, peer);
  byId.set(peer.id, peer);

  emit(peer, 'hello', { id: peer.id, d: peer.d });
  emit(peer, 'peers', others(peer).map(pub));
  others(peer).forEach((p) => emit(p, 'peer-joined', pub(peer)));

  // A real event (not a comment) so the app can see the stream is alive and reconnect if it goes quiet.
  peer.beat = setInterval(() => { sweep(); emit(peer, 'ping', { t: Date.now(), p: others(peer).map(pub) }); }, PING_MS);
  req.on('close', () => dropPeer(peer, false));
  res.on('close', () => dropPeer(peer, false));
  res.on('error', () => dropPeer(peer, false));
}

// The app calls this every few seconds. A device that stops calling is gone, even if its connection looks open.
async function handlePing(req, res) {
  let body;
  try { body = await readBody(req, 512); } catch (e) { return json(res, 400, { error: 'bad request' }); }
  const p = byId.get(String(body.id || ''));
  if (!p) return json(res, 404, { error: 'unknown device' });
  p.seen = Date.now(); p.hb = true;
  json(res, 200, { ok: true });
}

// Sent by the browser as the page closes, so the device disappears right away instead of lingering.
async function handleLeave(req, res) {
  try { const body = await readBody(req, 512); dropPeer(byId.get(String(body.id || '')), true); } catch (e) {}
  res.writeHead(204); res.end();
}

async function handleSignal(req, res) {
  let body;
  try { body = await readBody(req); } catch (e) { return json(res, 400, { error: 'bad request' }); }
  sweep();
  const from = byId.get(body.from), to = byId.get(body.to);
  if (!from || !to) return json(res, 404, { error: 'unknown device' });
  if (from.key !== to.key) return json(res, 403, { error: 'not nearby' });
  from.seen = Date.now();
  emit(to, 'signal', { from: from.id, data: body.data });
  json(res, 200, { ok: true });
}

async function handleRename(req, res) {
  let body;
  try { body = await readBody(req, 2048); } catch (e) { return json(res, 400, { error: 'bad request' }); }
  const peer = byId.get(body.id);
  if (!peer) return json(res, 404, { error: 'unknown device' });
  peer.name = clean(body.name, 24) || peer.name;
  register(peer.did, peer.name, peer.type, 0, '');
  others(peer).forEach((p) => emit(p, 'peer-updated', pub(peer)));
  json(res, 200, { ok: true });
}

function serveStatic(req, res, pathname) {
  let rel = decodeURIComponent(pathname);
  if (rel === '/') rel = '/index.html';
  const file = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!file.startsWith(PUBLIC_DIR + path.sep) || !PUBLIC_FILES.has(path.relative(PUBLIC_DIR, file))) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    return res.end('Not found');
  }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('Not found'); }
    const ext = path.extname(file);
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': 'no-cache',
      'X-Content-Type-Options': 'nosniff'
    });
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'GET' && url.pathname === '/events') return handleEvents(req, res, url);
  if (req.method === 'POST' && url.pathname === '/signal') return handleSignal(req, res);
  if (req.method === 'POST' && url.pathname === '/rename') return handleRename(req, res);
  if (req.method === 'POST' && url.pathname === '/ping') return handlePing(req, res);
  if (req.method === 'POST' && url.pathname === '/leave') return handleLeave(req, res);
  if (req.method === 'GET' && url.pathname === '/ice') return json(res, 200, { iceServers: ICE });
  if (req.method === 'GET' && url.pathname === '/names') return handleNames(req, res, url);
  if (req.method === 'GET' && url.pathname === '/health') return json(res, 200, { ok: true, devices: byId.size });
  if (req.method === 'GET' || req.method === 'HEAD') return serveStatic(req, res, url.pathname);
  res.writeHead(405); res.end();
});

// Removes devices that went away without saying goodbye (phone locked, signal lost, app killed). A device that has not
// checked in for STALE_MS is gone, whether or not its connection still looks open.
function sweep() {
  const now = Date.now();
  for (const p of [...byId.values()]) {
    const dead = p.res.destroyed || p.res.writableEnded;
    if (dead || now - p.seen > STALE_MS) dropPeer(p, true);
  }
}
setInterval(sweep, 5000).unref();

const ready = REMOTE
  ? loadRemote().catch((e) => console.error('Could not read the database, starting with what the files hold:', e.message))
  : Promise.resolve();
ready.then(() => server.listen(PORT, '0.0.0.0', () => {
  console.log('Ferry is running.');
  console.log('  Device names list: /names?key=' + (KEY_GIVEN ? '<your ADMIN_KEY>' : ADMIN_KEY + '   (new key each restart. Set ADMIN_KEY to keep one.)'));
  console.log('  On this computer:  http://localhost:' + PORT);
  Object.values(os.networkInterfaces()).flat()
    .filter((i) => i && i.family === 'IPv4' && !i.internal)
    .forEach((i) => console.log('  On your phones:    http://' + i.address + ':' + PORT + '   (same Wi-Fi)'));
  console.log('  Records are kept in: ' + DATA_DIR + (REMOTE ? ' and in the database' : '  (no database set: on a host that wipes its disk on restart, set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN)'));
}));
