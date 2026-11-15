
// Anime Clash Online Server
// Reconnect-safe WebSocket server with heartbeat and matchmaking.

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT) || 8080;
const MAX_PLAYERS = 4;
const HEARTBEAT_INTERVAL = 15000;
const PLAYER_TIMEOUT = 45000;

const LB_FILE = path.join(__dirname, 'leaderboard.json');

let leaderboard = {};

try {
  leaderboard = JSON.parse(fs.readFileSync(LB_FILE, 'utf8'));
} catch (e) {
  leaderboard = {};
}

function saveLeaderboard() {
  fs.writeFile(LB_FILE, JSON.stringify(leaderboard), () => {});
}

const players = new Map();
let queue = [];
let matchId = 0;

function names() {
  return [...players.values()].map(p => p.n);
}

function top() {
  return Object.values(leaderboard)
    .sort((a, b) => b.elo - a.elo)
    .slice(0, 10);
}

function send(c, data) {
  if (!c || c.closed || c.sock.destroyed || !c.sock.writable) return;

  const payload = Buffer.from(JSON.stringify(data));
  const length = payload.length;

  let header;

  if (length < 126) {
    header = Buffer.from([0x81, length]);
  } else if (length < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(length), 2);
  }

  try {
    c.sock.write(Buffer.concat([header, payload]));
  } catch (e) {
    drop(c);
  }
}

function broadcast(data) {
  for (const p of players.values()) {
    send(p.c, data);
  }
}

function busy(p) {
  return !!(p.match && !p.match.done);
}

function other(p) {
  return p.match && p.match.p.find(q => q !== p);
}

function startMatch(a, b) {
  const match = {
    id: ++matchId,
    p: [a, b],
    done: false
  };

  a.match = match;
  b.match = match;

  a.chal = null;
  b.chal = null;

  queue = queue.filter(p => p !== a && p !== b);

  send(a.c, {
    t: 'match',
    opp: b.n,
    slot: 0,
    id: match.id
  });

  send(b.c, {
    t: 'match',
    opp: a.n,
    slot: 1,
    id: match.id
  });
}

function record(winner, loser) {
  const wk = winner.toLowerCase();
  const lk = loser.toLowerCase();

  const a = leaderboard[wk] =
    leaderboard[wk] || { n: winner, elo: 1000, w: 0, l: 0 };

  const b = leaderboard[lk] =
    leaderboard[lk] || { n: loser, elo: 1000, w: 0, l: 0 };

  const expected = 1 / (1 + Math.pow(10, (b.elo - a.elo) / 400));

  a.elo = Math.round(a.elo + 32 * (1 - expected));
  b.elo = Math.max(0, Math.round(b.elo - 32 * (1 - expected)));

  a.w++;
  b.l++;

  saveLeaderboard();
}

function result(match, winnerSlot) {
  if (!match || match.done) return;

  match.done = true;

  const winner = match.p[winnerSlot];
  const loser = match.p[1 - winnerSlot];

  if (winner && loser) {
    record(winner.n, loser.n);
  }

  for (const p of match.p) {
    send(p.c, {
      t: 'lb',
      rows: top()
    });
  }
}

function leave(p) {
  if (!p) return;

  const match = p.match;

  if (!match) return;

  const opponent = other(p);

  if (!match.done && opponent) {
    result(match, match.p.indexOf(opponent));
  }

  if (opponent && opponent.match === match) {
    opponent.match = null;
    send(opponent.c, { t: 'opp_left' });
  }

  p.match = null;
}

function removeFromQueue(p) {
  queue = queue.filter(q => q !== p);
}

function drop(c) {
  if (!c || c.closed) return;

  c.closed = true;

  const p = c.p;
  c.p = null;

  if (!p) return;

  // Only remove this player if this exact connection owns the username.
  const key = p.n.toLowerCase();

  if (players.get(key) === p) {
    players.delete(key);
  }

  removeFromQueue(p);

  leave(p);

  if (p.chal) {
    const target = p.chal;
    p.chal = null;

    if (target.chal === p) {
      target.chal = null;
    }

    send(target.c, {
      t: 'cancel',
      from: p.n
    });
  }

  for (const otherPlayer of players.values()) {
    if (otherPlayer.chal === p) {
      otherPlayer.chal = null;

      send(otherPlayer.c, {
        t: 'cancel',
        from: p.n
      });
    }
  }

  broadcast({
    t: 'online',
    list: names()
  });
}

function handle(c, raw) {
  let m;

  try {
    m = JSON.parse(raw);
  } catch (e) {
    return;
  }

  let p = c.p;

  if (m.t === 'hello') {
    if (p || c.closed) return;

    const n = String(m.name || '')
      .replace(/[^A-Za-z0-9_]/g, '')
      .slice(0, 12);

    if (!n) {
      return send(c, {
        t: 'err',
        m: 'Bad username'
      });
    }

    const key = n.toLowerCase();
    const existing = players.get(key);

    if (existing && !existing.c.closed) {
      return send(c, {
        t: 'err',
        m: 'That username is already online'
      });
    }

    // Clean up any stale connection for this username.
    if (existing) {
      drop(existing.c);
    }

    if (players.size >= MAX_PLAYERS) {
      return send(c, {
        t: 'err',
        m: 'Server full (max ' + MAX_PLAYERS + ' players)'
      });
    }

    p = {
      n,
      c,
      match: null,
      chal: null
    };

    c.p = p;
    players.set(key, p);

    send(c, {
      t: 'welcome',
      online: names()
    });

    broadcast({
      t: 'online',
      list: names()
    });

    return;
  }

  if (!p || c.closed) return;

  // Resolve the target player for challenge-related messages.
  const targetName = m.to || m.from;
  const target = targetName
    ? players.get(String(targetName).toLowerCase())
    : null;

  switch (m.t) {
    case 'chal': {
      if (!target || target === p) {
        return send(c, {
          t: 'err',
          m: 'Player "' + m.to + '" is not online'
        });
      }

      if (busy(target) || busy(p)) {
        return send(c, {
          t: 'err',
          m: 'Player is busy'
        });
      }

      p.chal = target;
      target.chal = p;

      send(target.c, {
        t: 'chal',
        from: p.n
      });

      break;
    }

    case 'cancel': {
      if (target) {
        target.chal = null;

        send(target.c, {
          t: 'cancel',
          from: p.n
        });
      }

      p.chal = null;
      break;
    }

    case 'acc': {
      if (!target || target.chal !== p ||
          p.chal !== target || busy(target) || busy(p)) {
        return send(c, {
          t: 'err',
          m: 'Challenge expired'
        });
      }

      startMatch(target, p);
      break;
    }

    case 'dec': {
      if (target) {
        target.chal = null;
        p.chal = null;

        send(target.c, {
          t: 'declined',
          by: p.n
        });
      }

      break;
    }

    case 'q': {
      if (busy(p) || queue.includes(p)) return;

      removeFromQueue(p);

      const opponent = queue.find(q => q !== p && !busy(q));

      if (opponent) {
        startMatch(opponent, p);
      } else {
        queue.push(p);
      }

      break;
    }

    case 'unq': {
      removeFromQueue(p);
      break;
    }

    case 'pick':
    case 'start':
    case 'in':
    case 'sync': {
      const opponent = other(p);

      if (opponent && !opponent.c.closed) {
        send(opponent.c, m);
      }

      break;
    }

    case 'res': {
      if (p.match && !p.match.done) {
        result(p.match, m.w == 1 ? 1 : 0);
      }

      break;
    }

    case 'leave': {
      leave(p);
      break;
    }

    case 'disconnect': {
      drop(c);

      try {
        c.sock.end();
      } catch (e) {}

      break;
    }

    case 'lb': {
      send(c, {
        t: 'lb',
        rows: top()
      });

      break;
    }
  }
}

// Minimal WebSocket frame parser.
function parse(c) {
  while (!c.closed) {
    const b = c.buf;

    if (b.length < 2) return;

    const opcode = b[0] & 15;
    const masked = !!(b[1] & 128);

    let length = b[1] & 127;
    let offset = 2;

    if (length === 126) {
      if (b.length < 4) return;

      length = b.readUInt16BE(2);
      offset = 4;
    } else if (length === 127) {
      if (b.length < 10) return;

      const bigLength = b.readBigUInt64BE(2);

      if (bigLength > 1000000n) {
        c.sock.destroy();
        return;
      }

      length = Number(bigLength);
      offset = 10;
    }

    if (length > 1000000) {
      c.sock.destroy();
      return;
    }

    let mask;

    if (masked) {
      if (b.length < offset + 4) return;

      mask = b.subarray(offset, offset + 4);
      offset += 4;
    }

    if (b.length < offset + length) return;

    const data = Buffer.from(b.subarray(offset, offset + length));

    if (masked) {
      for (let i = 0; i < data.length; i++) {
        data[i] ^= mask[i & 3];
      }
    }

    c.buf = b.subarray(offset + length);

    if (opcode === 8) {
      drop(c);
      c.sock.end();
      return;
    }

    if (opcode === 9) {
      const pong = Buffer.concat([
        Buffer.from([0x8A, data.length]),
        data
      ]);

      c.sock.write(pong);
      continue;
    }

    if (opcode === 10) {
      c.lastPong = Date.now();
      continue;
    }

    if (opcode === 1) {
      handle(c, data.toString());
    }
  }
}

// HTTP health check.
const server = http.createServer((req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/plain'
  });

  res.end(
    'Anime Clash server is running. Players online: ' +
    players.size + '/' + MAX_PLAYERS
  );
});

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];

  if (!key) {
    sock.destroy();
    return;
  }

  const accept = crypto
    .createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');

  sock.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );

  sock.setNoDelay(true);

  const c = {
    sock,
    buf: Buffer.alloc(0),
    p: null,
    closed: false,
    lastPong: Date.now()
  };

  sock.on('data', data => {
    if (c.closed) return;

    c.buf = Buffer.concat([c.buf, data]);
    parse(c);
  });

  sock.on('close', () => drop(c));

  sock.on('error', () => {
    drop(c);
    sock.destroy();
  });
});

// Detect connections that have stopped responding.
const heartbeat = setInterval(() => {
  const now = Date.now();

  for (const p of players.values()) {
    const c = p.c;

    if (c.closed) {
      drop(c);
      continue;
    }

    if (now - c.lastPong > PLAYER_TIMEOUT) {
      drop(c);
      c.sock.destroy();
      continue;
    }

    try {
      c.sock.write(Buffer.from([0x89, 0x00]));
    } catch (e) {
      drop(c);
      c.sock.destroy();
    }
  }
}, HEARTBEAT_INTERVAL);

heartbeat.unref();

server.listen(PORT, '0.0.0.0', () => {
  console.log(
    'Anime Clash server running on port ' + PORT +
    ' (max ' + MAX_PLAYERS + ' players)'
  );
});
