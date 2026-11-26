// Anime Clash online server - low-latency relay
// Supports: 1v1 ranked (queue + challenges, ELO leaderboard), 3-player free-for-all queue,
// private rooms of up to 4 (host settings, invites, ban list - unranked), WebRTC signalling and admin unlock gifts.
//
// Settings (environment variables, all optional):
//   PORT        port to listen on (default 8080)
//   MAX         max players online at once (default 30)
//   ADMIN_KEY   admin code - must match ADMIN_CODE in index.html (default 'clash-admin')
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = +process.env.PORT || 8080;
const MAX = +process.env.MAX || 30;
const ADMIN_KEY = process.env.ADMIN_KEY || 'clash-admin';
const FILE = path.join(__dirname, 'leaderboard.json');

/* fighters banned from ranked (1v1 online) matches, by their index in the game's fighter list.
   25 = Neshama Tehora. The game also hides them from the ranked pick screen; this is the server-side safety net. */
const RANKED_BANNED = new Set([25]);
const BAN_MSG = 'NESHAMA TEHORA IS BANNED FROM RANKED';

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
let queue = [];   // waiting for a 1v1 ranked match
let queue3 = [];  // waiting for a 3-player free-for-all
let mid = 0;
const rooms = new Map(); // code -> { code, host, ps:[players], set:{gm,rw,tm,st,ban}, game }

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
// everyone else still in this player's match
const others = p => (p.match ? p.match.p.filter(q => q !== p && q.match === p.match) : []);
const other = p => others(p)[0];

// relay a raw message to everyone else in the match (no re-encoding of the JSON)
function relay(p, raw) {
  const os = others(p);
  if (!os.length) return;
  const buf = encodeFrame(raw);
  for (const q of os) sendRaw(q.c, buf);
}

const unqueue = p => {
  queue = queue.filter(q => q !== p);
  if (queue3.includes(p)) {
    queue3 = queue3.filter(q => q !== p);
    q3count();
  }
};
const q3count = () => { for (const q of queue3) send(q.c, { t: 'q3n', n: queue3.length }); };

function startMatch(a, b, opt = { ranked: true }) {
  const m = { id: ++mid, p: [a, b], np: 2, done: false, ranked: !!opt.ranked, room: opt.room || null, rs: opt.room ? opt.room.set : null };
  a.match = b.match = m;
  a.chal = b.chal = null;
  unqueue(a);
  unqueue(b);
  const ex = { ranked: m.ranked, rs: m.rs, room: m.room && m.room.code };
  send(a.c, { t: 'match', opp: b.n, slot: 0, id: m.id, np: 2, names: [a.n, b.n], ...ex });
  send(b.c, { t: 'match', opp: a.n, slot: 1, id: m.id, np: 2, names: [a.n, b.n], ...ex });
}

function startMatch3(ps, opt = {}) {
  const m = { id: ++mid, p: ps, np: ps.length, done: false, ranked: false, room: opt.room || null, rs: opt.room ? opt.room.set : null };
  const nm = ps.map(q => q.n);
  ps.forEach(q => { q.match = m; q.chal = null; unqueue(q); });
  ps.forEach((q, i) => send(q.c, { t: 'match', np: ps.length, slot: i, id: m.id, names: nm, opp: nm.filter((_, j) => j != i).join(' & '), ranked: false, rs: m.rs, room: m.room && m.room.code }));
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

/* only ranked 1v1 matches (queue / challenge) change ELO; rooms and 3-player free-for-alls don't */
function result(m, w) {
  if (m.done) return;
  m.done = true;
  if (!m.ranked) return;
  record(m.p[w].n, m.p[1 - w].n);
  const rows = top();
  for (const q of m.p) send(q.c, { t: 'lb', rows });
}

function leave(p) {
  const m = p.match;
  if (!m) return;
  const os = others(p);
  if (m.np === 2) {
    const o = os[0];
    if (!m.done && o) result(m, m.p.indexOf(o)); // leaving a ranked match = forfeit
    if (o) {
      send(o.c, { t: 'opp_left' });
      if (o.match === m) o.match = null; // a 1v1 is over once either player leaves
    }
  } else {
    m.done = true; // the game ends a 3-player match for everyone when someone leaves
    const s = m.p.indexOf(p);
    for (const q of os) send(q.c, { t: 'opp_left', s, who: p.n });
  }
  p.match = null;
  roomMatchOver(m);
}

/* ---------- private rooms ---------- */
const ROOM_MAX = 4;
const roomState = r => ({ t: 'room', code: r.code, host: r.host.n, players: r.ps.map(q => q.n), set: r.set, game: r.game });
const rsend = r => { const o = roomState(r); for (const q of r.ps) send(q.c, o); };
function roomCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
  for (;;) { let c = ''; for (let i = 0; i < 4; i++) c += A[Math.floor(Math.random() * A.length)]; if (!rooms.has(c)) return c; }
}
function roomLeave(p, why) {
  const r = p.room;
  if (!r) return;
  r.ps = r.ps.filter(q => q !== p);
  p.room = null;
  if (why) send(p.c, { t: 'rleft', m: why });
  if (!r.ps.length) { rooms.delete(r.code); return; }
  if (r.host === p) r.host = r.ps[0];
  rsend(r);
}
/* when everyone has left a room's match, the room is free again and everyone sees it */
function roomMatchOver(m) {
  const r = m.room;
  if (!r || !r.game || m.p.some(q => q.match === m)) return;
  r.game = false;
  if (rooms.get(r.code) === r) rsend(r);
}
const clampSet = (v, ok, d) => (ok.includes(v) ? v : d);
function cleanSet(s) {
  s = s && typeof s === 'object' ? s : {};
  const ban = Array.isArray(s.ban) ? [...new Set(s.ban.map(v => v | 0).filter(v => v >= 0 && v < 64))].slice(0, 40) : [];
  return {
    gm: s.gm === 'candle' ? 'candle' : '',
    rw: clampSet(s.rw | 0, [1, 2, 3], 2),
    tm: clampSet(s.tm | 0, [0, 60, 90, 120, 180], 0),
    st: Math.max(-2, Math.min(15, Number.isInteger(s.st) ? s.st : -1)),
    ban,
  };
}

// Fast-path relay for real-time combat packets without JSON.parse overhead
function fastRelay(c, raw) {
  const p = c.p;
  if (!p || !p.match) return false;
  if (raw.startsWith('{"t":"in"') || raw.startsWith('{"t":"sync"')) {
    relay(p, raw);
    return true;
  }
  return false;
}

const ranked = p => !!(p.match && p.match.ranked);
const roomBanned = (p, c) => !!(p.match && p.match.rs && p.match.rs.ban.includes(+c));

function handle(c, s) {
  if (fastRelay(c, s)) return;

  let m;
  try { m = JSON.parse(s); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
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
    p = c.p = { n, c, match: null, chal: null, adm: false, room: null };
    players.set(n.toLowerCase(), p);
    send(c, { t: 'welcome', online: names() });
    bcast({ t: 'online', list: names() });
    return;
  }

  if (!p) return;
  const who = m.to != null && typeof m.to !== 'number' ? m.to : m.from;
  const T = who ? players.get(String(who).toLowerCase()) : null;

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
      if (p.room) return send(c, { t: 'err', m: 'Leave your room first' });
      if (busy(p) || queue.includes(p)) return;
      unqueue(p);
      const o = queue.find(q => q !== p);
      if (o) startMatch(o, p);
      else queue.push(p);
      break;
    }
    case 'q3': {
      if (p.room) return send(c, { t: 'err', m: 'Leave your room first' });
      if (busy(p) || queue3.includes(p)) return;
      unqueue(p);
      queue3.push(p);
      if (queue3.length >= 3) startMatch3(queue3.slice(0, 3));
      else q3count();
      break;
    }
    case 'unq':
      unqueue(p);
      break;
    case 'pick':
      if (!p.match) return;
      if (ranked(p) && RANKED_BANNED.has(+m.c)) return send(c, { t: 'err', m: BAN_MSG });
      if (roomBanned(p, m.c)) return send(c, { t: 'err', m: 'That fighter is banned in this room' });
      relay(p, s);
      break;
    case 'start':
      if (!p.match) return;
      if (ranked(p) && Array.isArray(m.c) && m.c.some(v => RANKED_BANNED.has(+v))) return send(c, { t: 'err', m: BAN_MSG });
      if (Array.isArray(m.c) && m.c.some(v => roomBanned(p, v))) return send(c, { t: 'err', m: 'A banned fighter was picked' });
      relay(p, s);
      break;
    case 'rtc': {
      if (!p.match) return;
      if (p.match.np >= 3 && typeof m.to === 'number') {
        const q = p.match.p[m.to];
        if (q && q !== p && q.match === p.match) sendRaw(q.c, encodeFrame(s)); // 3-4 players: signalling goes to one specific player
      } else relay(p, s);
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
    /* ----- private rooms ----- */
    case 'rcreate': {
      if (busy(p)) return send(c, { t: 'err', m: 'Finish your match first' });
      roomLeave(p);
      unqueue(p);
      const r = { code: roomCode(), host: p, ps: [p], set: cleanSet({}), game: false };
      rooms.set(r.code, r);
      p.room = r;
      rsend(r);
      break;
    }
    case 'rjoin': {
      const r = rooms.get(String(m.code || '').toUpperCase().trim());
      if (!r) return send(c, { t: 'err', m: 'No room with code "' + String(m.code || '').slice(0, 8) + '"' });
      if (r === p.room) return rsend(r);
      if (r.ps.length >= ROOM_MAX) return send(c, { t: 'err', m: 'That room is full (4 players)' });
      if (r.game) return send(c, { t: 'err', m: 'That room is in a match - try again soon' });
      if (busy(p)) return send(c, { t: 'err', m: 'Finish your match first' });
      roomLeave(p);
      unqueue(p);
      r.ps.push(p);
      p.room = r;
      rsend(r);
      break;
    }
    case 'rleave':
      roomLeave(p);
      break;
    case 'rinv': {
      const r = p.room;
      if (!r) return;
      const tg = players.get(String(m.to || '').toLowerCase());
      if (!tg || tg === p) return send(c, { t: 'rmsg', m: 'Player "' + String(m.to || '').slice(0, 12) + '" is not online' });
      if (r.ps.includes(tg)) return send(c, { t: 'rmsg', m: tg.n + ' is already in the room' });
      send(tg.c, { t: 'rinv', from: p.n, code: r.code });
      send(c, { t: 'rmsg', m: 'Invite sent to ' + tg.n });
      break;
    }
    case 'rset': {
      const r = p.room;
      if (!r || r.host !== p || r.game) return;
      r.set = cleanSet(m.set);
      rsend(r);
      break;
    }
    case 'rkick': {
      const r = p.room;
      if (!r || r.host !== p || r.game) return;
      const tg = r.ps.find(q => q.n.toLowerCase() === String(m.name || '').toLowerCase());
      if (tg && tg !== p) roomLeave(tg, 'You were removed from room ' + r.code);
      break;
    }
    case 'rstart': {
      const r = p.room;
      if (!r || r.host !== p || r.game) return;
      if (r.ps.length < 2) return send(c, { t: 'rmsg', m: 'You need at least 2 players' });
      if (r.ps.some(q => busy(q))) return send(c, { t: 'rmsg', m: 'Someone is still in another match' });
      r.game = true;
      const ps = [r.host, ...r.ps.filter(q => q !== r.host)]; // the host is slot 0 (picks the stage)
      if (ps.length === 2) startMatch(ps[0], ps[1], { ranked: false, room: r });
      else startMatch3(ps.slice(0, ROOM_MAX), { room: r }); // 3 or 4 players: free-for-all
      rsend(r);
      break;
    }
    case 'admin':
      p.adm = m.key === ADMIN_KEY;
      send(c, { t: 'adm', m: p.adm ? 'Admin ON on this server.' : 'Wrong admin code for this server.' });
      break;
    case 'give': {
      if (!p.adm) return send(c, { t: 'adm', m: 'Not an admin on this server.' });
      const tg = players.get(String(m.to || '').toLowerCase());
      if (!tg) return send(c, { t: 'adm', m: 'Player "' + m.to + '" is not online.' });
      const ids = Array.isArray(m.c) ? m.c.map(String).slice(0, 64) : [];
      if (!ids.length) return;
      send(tg.c, { t: 'gift', c: ids, take: !!m.take, by: p.n });
      send(c, { t: 'adm', m: 'Sent to ' + tg.n + '.' });
      break;
    }
  }
}

function drop(c) {
  const p = c.p;
  if (!p) return;
  c.p = null;
  leave(p);
  roomLeave(p);
  players.delete(p.n.toLowerCase());
  unqueue(p);
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

    const d = Buffer.allocUnsafe(len);
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

// 15-second heartbeat ping prevents proxy idle drops and cleans dead sockets
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
  console.log('Anime Clash server running on port ' + PORT + ' (max ' + MAX + ' players)');
});
