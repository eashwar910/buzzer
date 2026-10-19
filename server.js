const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const QRCode = require('qrcode');

const PUB = path.join(__dirname, 'public');
const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.woff2': 'font/woff2' };
// Sub-millisecond wall clock; clients use the same formula so offsets line up.
const now = () => performance.timeOrigin + performance.now();

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/qr') {
    const text = url.searchParams.get('text') || '';
    if (!text || text.length > 500) { res.writeHead(400); return res.end(); }
    res.writeHead(200, { 'Content-Type': 'image/svg+xml' });
    return res.end(await QRCode.toString(text, { type: 'svg', margin: 1 }));
  }
  const file = path.join(PUB, path.normalize(url.pathname === '/' ? '/index.html' : url.pathname));
  if (!file.startsWith(PUB + path.sep)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
});

// shortcut: rooms live in memory, so a server restart ends every game; use Redis if that matters.
const rooms = new Map();
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function newCode() {
  let c;
  do c = Array.from({ length: 5 }, () => ALPHABET[crypto.randomInt(ALPHABET.length)]).join('');
  while (rooms.has(c));
  return c;
}
const cleanName = n => String(n ?? '').trim().slice(0, 24);
const send = (ws, m) => ws.readyState === 1 && ws.send(JSON.stringify(m));

function broadcast(room) {
  const s = JSON.stringify({
    t: 'state', code: room.code, open: room.open,
    players: [...room.players].map(([id, p]) => ({ id, name: p.name, score: p.score, online: p.sockets.size > 0 })),
    buzzes: room.buzzes.map(b => ({ id: b.id, ms: b.at - room.openedAt })),
  });
  for (const ws of room.sockets) ws.readyState === 1 && ws.send(s);
}

function handle(ws, m) {
  if (m.t === 'ping') return send(ws, { t: 'pong', c: m.c, s: now() });
  if (m.t === 'spong') { if (m.s === ws.pingS) ws.rtt = now() - m.s; return; }
  if (m.t === 'create') {
    const room = { code: newCode(), hostToken: crypto.randomUUID(), players: new Map(), open: false, openedAt: 0, buzzes: [], sockets: new Set(), touched: Date.now() };
    rooms.set(room.code, room);
    return send(ws, { t: 'created', code: room.code, hostToken: room.hostToken });
  }
  if (m.t === 'host' || m.t === 'join') {
    const room = rooms.get(String(m.code).toUpperCase());
    if (!room) return send(ws, { t: 'ended' });
    if (m.t === 'host') {
      if (m.hostToken !== room.hostToken) return send(ws, { t: 'error', msg: 'You are not the host of this room.' });
      ws.isHost = true;
    } else {
      let id = m.playerId, p = room.players.get(id);
      if (!p) {
        const name = cleanName(m.name);
        if (!name) return send(ws, { t: 'error', msg: 'Enter a name.' });
        id = crypto.randomUUID();
        p = { name, score: 0, sockets: new Set() };
        room.players.set(id, p);
      }
      p.sockets.add(ws);
      ws.playerId = id;
      send(ws, { t: 'joined', playerId: id });
    }
    ws.room = room;
    room.sockets.add(ws);
    return broadcast(room);
  }

  const room = ws.room;
  if (!room) return;
  room.touched = Date.now();
  const me = room.players.get(ws.playerId);

  if (me && m.t === 'rename') {
    const name = cleanName(m.name);
    if (name) me.name = name;
    return broadcast(room);
  }
  if (me && m.t === 'buzz') {
    if (!room.open || room.buzzes.some(b => b.id === ws.playerId)) return;
    const arrival = now();
    // Trust the client's clock-synced press time, but only within one measured round trip,
    // so a tampered client can't claim it pressed earlier than the network allows.
    const at = Math.min(arrival, Math.max(Number(m.at) || arrival, arrival - ws.rtt, room.openedAt));
    room.buzzes.push({ id: ws.playerId, at });
    room.buzzes.sort((a, b) => a.at - b.at);
    return broadcast(room);
  }

  if (!ws.isHost) return;
  if (m.t === 'open') { room.open = true; room.openedAt = now(); room.buzzes = []; }
  else if (m.t === 'lock') room.open = false;
  else if (m.t === 'score') {
    const p = room.players.get(m.playerId), d = Math.trunc(Number(m.delta));
    if (p && Number.isFinite(d) && Math.abs(d) <= 10000) p.score += d;
  } else if (m.t === 'end') {
    rooms.delete(room.code);
    for (const s of room.sockets) { send(s, { t: 'ended' }); s.room = null; }
    return;
  }
  broadcast(room);
}

function leave(ws) {
  const room = ws.room;
  if (!room) return;
  room.sockets.delete(ws);
  room.players.get(ws.playerId)?.sockets.delete(ws);
  room.touched = Date.now();
  broadcast(room);
}

const wss = new WebSocketServer({ server });
function ping(ws) { ws.pingS = now(); send(ws, { t: 'sping', s: ws.pingS }); }
wss.on('connection', ws => {
  ws.rtt = 0;
  ping(ws);
  ws.on('message', raw => { let m; try { m = JSON.parse(raw); } catch { return; } if (m && typeof m === 'object') handle(ws, m); });
  ws.on('close', () => leave(ws));
});
setInterval(() => wss.clients.forEach(ping), 2000).unref();
// Abandoned rooms (nobody connected for 12h) are dropped so memory doesn't grow forever.
setInterval(() => {
  for (const [code, r] of rooms) if (!r.sockets.size && Date.now() - r.touched > 12 * 3600e3) rooms.delete(code);
}, 600e3).unref();

module.exports = server;

if (require.main === module) {
  const PORT = process.env.PORT || 3000;
  server.listen(PORT, () => {
    console.log(`Buzzer running on http://localhost:${PORT}`);
    for (const a of Object.values(os.networkInterfaces()).flat())
      if (a.family === 'IPv4' && !a.internal) console.log(`  on your network: http://${a.address}:${PORT}  (host from this URL so the QR works on phones)`);
  });
}
