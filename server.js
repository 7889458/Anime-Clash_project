// Anime Clash online server - Low-Latency Optimized
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const os = require('os');

const PORT = +process.env.PORT || 8080;
const MAX = 4;
const FILE = path.join(__dirname, 'leaderboard.json');

let lb = {};
try { lb = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch (e) {}

// Debounced async leaderboard save so disk I/O never stalls the network loop
let saveTimer = null;
const saveLB = () => {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    fs.writeFile(FILE, JSON.stringify(lb), () => {});
  }, 1500);
};

const players = new Map();
let queue = [];
let mid = 0;

const names = () => [...players.values()].map(p => p.n);
const top = () => Object.values(lb).sort((a, b) => b.elo - a.elo).slice(0, 10);

// Fast WebSocket frame builder
function encodeFrame(str) {
  const d = Buffer.from(str);
  const n = d.length;
  let h;
  if (n < 126) {
    h = Buffer.from([0x81, n]);
  } else if (n < 65536) {
    h = Buffer.allocUnsafe(4);
    h[0] = 0x81;
    h[1] = 126;
    h.writeUInt16BE(n, 2);
  } else {
    h = Buffer.allocUnsafe(10);
    h[0] = 0x81;
    h[1] = 127;
    h.writeBigUInt64BE(BigInt(n), 2);
  }
  return Buffer.concat([h, d]);
}

function sendRaw(c, buf) {
  if (!c || c.sock.destroyed || !c.sock.writable) return;
  try {
    c.sock.write(buf);
  } catch (e) {
    drop(c);
  }
}

function send(c, o) {
  sendRaw(c, encodeFrame(JSON.stringify(o)));
}

const bcast = o => {
  const buf = encodeFrame(JSON.stringify(o));
  for (const p of players.values()) sendRaw(p.c, buf);
};

const busy = p => p.match && !p.match.done;
const other = p => p.match && p.match.p.find(q => q !== p);

function startMatch(a, b) {
  const m = { id: ++mid, p: [a, b], done: false };
  a.match = b.match = m;
  a.chal = b.chal = null;
  queue = queue.filter(q => q !== a && q !== b);
  send(a.c, { t: 'match', opp: b.n, slot: 0, id: m.id });
  send(b.c, { t: 'match', opp: a.n, slot: 1, id: m.id });
}

function record(w, l) {
  const kw = w.toLowerCase(), kl = l.toLowerCase();
  const a = lb[kw] = lb[kw] || { n: w, elo: 1000, w: 0, l: 0 };
  const b = lb[kl] = lb[kl] || { n: l, elo: 1000, w: 0, l: 0 };
  const e = 1 / (1 + Math.pow(10, (b.elo - a.elo) / 400));
  a.elo = Math.round(a.elo + 32 * (1 - e));
  b.elo = Math.max(0, Math.round(b.elo - 32 * (1 - e)));
  a.w++;
  b.l++;
  saveLB();
}

function result(m, w) {
  if (m.done) return;
  m.done = true;
  record(m.p[w].n, m.p[1 - w].n);
  const rows = top();
  for (const q of m.p) send(q.c, { t: 'lb', rows });
}

function leave(p) {
  const m = p.match;
  if (!m) return;
  const o = other(p);
  if (!m.done && o) result(m, m.p.indexOf(o));
  if (o) {
    send(o.c, { t: 'opp_left' });
    if (o.match === m) o.match = null;
  }
  p.match = null;
}

// Fast-path relay for real-time combat packets without JSON.parse overhead
function fastRelay(c, rawStr) {
  const p = c.p;
  if (!p || !p.match) return false;
  // Check for high-frequency combat types: "in", "sync", "pick", "start"
  if (
    rawStr.startsWith('{"t":"in"') ||
    rawStr.startsWith('{"t":"sync"') ||
    rawStr.startsWith('{"t":"pick"') ||
    rawStr.startsWith('{"t":"start"')
  ) {
    const o = other(p);
    if (o) sendRaw(o.c, encodeFrame(rawStr));
    return true;
  }
  return false;
}

function handle(c, s) {
  // Zero-parse fast relay for combat inputs & state sync
  if (fastRelay(c, s)) return;

  let m;
  try { m = JSON.parse(s); } catch (e) { return; }
  let p = c.p;

  if (m.t === 'ping') {
    return send(c, { t: 'pong', ts: m.ts });
  }

  if (m.t === 'hello') {
    if (p) return;
    const n = String(m.name || '').replace(/[^A-Za-z0-9_]/g, '').slice(0, 12);
    if (!n) return send(c, { t: 'err', m: 'Bad username' });

    const old = players.get(n.toLowerCase());
    if (old) {
      drop(old.c);
      try { old.c.sock.destroy(); } catch (e) {}
    }

    if (players.size >= MAX) return send(c, { t: 'err', m: 'Server full (max ' + MAX + ' players)' });
    p = c.p = { n, c, match: null, chal: null };
    players.set(n.toLowerCase(), p);
    send(c, { t: 'welcome', online: names() });
    bcast({ t: 'online', list: names() });
    return;
  }

  if (!p) return;
  const T = m.to || m.from ? players.get(String(m.to || m.from).toLowerCase()) : null;

  switch (m.t) {
    case 'chal':
      if (!T || T === p) return send(c, { t: 'err', m: 'Player "' + m.to + '" is not online' });
      if (busy(T) || busy(p)) return send(c, { t: 'err', m: 'Player is busy' });
      p.chal = T;
      send(T.c, { t: 'chal', from: p.n });
      break;
    case 'cancel':
      if (T) send(T.c, { t: 'cancel', from: p.n });
      p.chal = null;
      break;
    case 'acc':
      if (!T || T.chal !== p || busy(T) || busy(p)) return send(c, { t: 'err', m: 'Challenge expired' });
      startMatch(T, p);
      break;
    case 'dec':
      if (T) {
        T.chal = null;
        send(T.c, { t: 'declined', by: p.n });
      }
      break;
    case 'q': {
      if (busy(p) || queue.includes(p)) return;
      const o = queue.find(q => q !== p);
      if (o) startMatch(o, p);
      else queue.push(p);
      break;
    }
    case 'unq':
      queue = queue.filter(q => q !== p);
      break;
    case 'pick':
    case 'start':
    case 'in':
    case 'sync': {
      const o = other(p);
      if (o) sendRaw(o.c, encodeFrame(s));
      break;
    }
    case 'res':
      if (p.match && !p.match.done) result(p.match, m.w == 1 ? 1 : 0);
      break;
    case 'leave':
      leave(p);
      break;
    case 'bye':
      drop(c);
      try { c.sock.destroy(); } catch (e) {}
      return;
    case 'lb':
      send(c, { t: 'lb', rows: top() });
      break;
  }
}

function drop(c) {
  const p = c.p;
  if (!p) return;
  c.p = null;
  leave(p);
  players.delete(p.n.toLowerCase());
  queue = queue.filter(q => q !== p);
  bcast({ t: 'online', list: names() });
}

function parse(c) {
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    let len = b[1] & 127, off = 2;
    if (len === 126) {
      if (b.length < 4) return;
      len = b.readUInt16BE(2);
      off = 4;
    } else if (len === 127) {
      if (b.length < 10) return;
      len = Number(b.readBigUInt64BE(2));
      off = 10;
    }
    if (len > 1e6) return c.sock.destroy();
    const mk = b[1] & 128;
    if (mk) off += 4;
    if (b.length < off + len) return;

    let d = Buffer.allocUnsafe(len);
    b.copy(d, 0, off, off + len);
    if (mk) {
      const k = b.subarray(off - 4, off);
      for (let i = 0; i < d.length; i++) d[i] ^= k[i & 3];
    }

    const op = b[0] & 15;
    c.buf = b.subarray(off + len);
    c.alive = true;

    if (op === 8) {
      drop(c);
      c.sock.destroy();
      return;
    } else if (op === 9) {
      sendRaw(c, Buffer.concat([Buffer.from([0x8a, d.length]), d]));
    } else if (op === 10) {
      c.alive = true;
    } else if (op === 1) {
      handle(c, d.toString());
    }
  }
}

const clients = new Set();

const server = http.createServer((q, r) => {
  r.writeHead(200, { 'Content-Type': 'text/plain', 'Cache-Control': 'no-store' });
  r.end('Anime Clash server running. Online: ' + players.size + '/' + MAX);
});

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) return sock.destroy();
  sock.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' +
    crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64') +
    '\r\n\r\n'
  );
  sock.setNoDelay(true);
  sock.setKeepAlive(true, 10000);

  const c = { sock, buf: Buffer.alloc(0), p: null, alive: true };
  clients.add(c);

  sock.on('data', d => {
    c.buf = c.buf.length ? Buffer.concat([c.buf, d]) : d;
    parse(c);
  });
  sock.on('close', () => { clients.delete(c); drop(c); });
  sock.on('end', () => { clients.delete(c); drop(c); });
  sock.on('error', () => { clients.delete(c); drop(c); sock.destroy(); });
});

// 15-second heartbeat ping prevents Render proxy idle drops and cleans dead sockets
setInterval(() => {
  const pingFrame = Buffer.from([0x89, 0x00]);
  for (const c of clients) {
    if (!c.alive) {
      clients.delete(c);
      drop(c);
      try { c.sock.destroy(); } catch (e) {}
      continue;
    }
    c.alive = false;
    sendRaw(c, pingFrame);
  }
}, 15000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('Anime Clash optimized server running on port ' + PORT);
});
