'use strict';
/**
 * JPEDG3D online server — rooms with private codes, max 5 players, server-authoritative timing, relay of car state.
 *   npm install && npm start            (PORT env var, default 8080)
 * It also serves the game client (index.html / jpedg3d_fase7.html in this folder) so one deployment is enough.
 */
const http = require('http'), fs = require('fs'), path = require('path');
let WebSocketServer;
try { ({ WebSocketServer } = require('ws')); } catch (e) { console.error('Missing dependency "ws". Run: npm install'); process.exit(1); }

const PORT = +process.env.PORT || 8080;
const MAX_PLAYERS = 5, COUNTDOWN_MS = 7500, TICK_MS = 50, MAX_MSG_PER_SEC = 80;
const RACE_TIMEOUT_MS = 20 * 60 * 1000, FINISH_GRACE_MS = 30000, IDLE_MS = 30000;
const LAPS = [3, 5, 8, 12], CODE_CH = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const rooms = new Map();
let uid = 0;
const now = () => Date.now();

/* ---------- helpers ---------- */
const clean = (s, d) => (String(s == null ? d : s).replace(/[^\p{L}\p{N} _.\-]/gu, '').trim().slice(0, 12)) || d;
const int = (v, lo, hi, d) => { v = Math.round(+v); return Number.isFinite(v) && v >= lo && v <= hi ? v : d; };
const send = (p, o) => { try { if (p.ws.readyState === 1) p.ws.send(JSON.stringify(o)); } catch (e) { /* socket closing */ } };
const shuffle = a => { for (let i = a.length - 1; i > 0; i--) { const j = Math.random() * (i + 1) | 0; [a[i], a[j]] = [a[j], a[i]]; } return a; };
function newCode() { for (;;) { let c = ''; for (let i = 0; i < 5; i++) c += CODE_CH[Math.random() * CODE_CH.length | 0]; if (!rooms.has(c)) return c; } }

function roomInfo(r) {
  return { t: 'room', code: r.code, host: r.host, phase: r.phase, settings: r.settings,
    players: [...r.players.values()].map(p => ({ id: p.id, name: p.name, team: p.team, ready: p.ready, ping: p.ping })) };
}
const broadcast = (r, o, except) => { for (const p of r.players.values()) if (p.id !== except) send(p, o); };
const broadcastRoom = r => broadcast(r, roomInfo(r));

/* ---------- room lifecycle ---------- */
function createRoom(p) {
  leaveRoom(p);
  const r = { code: newCode(), host: p.id, players: new Map(), phase: 'lobby', startAt: 0, bots: [], timers: [],
    settings: { track: 0, laps: 3, assists: 1, bots: true, diff: 1 } };
  rooms.set(r.code, r); addPlayer(r, p); p.ready = true; return r;
}
function addPlayer(r, p) { p.room = r; p.ready = false; p.fin = null; p.lap = 0; p.best = 0; p.st = null; r.players.set(p.id, p); }
function leaveRoom(p, reason) {
  const r = p.room; if (!r) return;
  r.players.delete(p.id); p.room = null;
  if (!r.players.size) { r.timers.forEach(clearTimeout); rooms.delete(r.code); return; }
  if (r.host === p.id) { r.host = r.players.keys().next().value; r.players.get(r.host).ready = true; r.bots = []; }
  broadcast(r, { t: 'left', id: p.id, name: p.name, why: reason || 'leave' });
  broadcastRoom(r);
  if (r.phase === 'race') checkFinish(r);
}
function checkFinish(r) {
  if (r.phase !== 'race') return;
  const ps = [...r.players.values()];
  if (ps.length && ps.every(p => p.fin != null)) sendResults(r);
}
function sendResults(r) {
  if (r.phase !== 'race') return;
  r.timers.forEach(clearTimeout); r.timers = [];
  const ps = [...r.players.values()];
  ps.sort((a, b) => (a.fin != null) === (b.fin != null) ? (a.fin != null ? a.fin - b.fin : progOf(b) - progOf(a)) : (a.fin != null ? -1 : 1));
  const rows = ps.map((p, i) => ({ id: p.id, name: p.name, team: p.team, t: p.fin, best: p.best || null, pos: i + 1, dnf: p.fin == null }));
  broadcast(r, { t: 'res', rows });
  r.phase = 'lobby'; r.bots = [];
  for (const p of ps) { p.ready = p.id === r.host; p.st = null; }
  broadcastRoom(r);
}
const progOf = p => (p.st ? p.st[12] * 10000 + p.st[10] : -1);   // lap * 10000 + track index

function startRace(r) {
  const ps = shuffle([...r.players.values()]), H = ps.length;
  const nb = r.settings.bots ? Math.max(0, 8 - H) : 0, T = H + nb;
  const grid = new Array(T).fill(null);
  ps.forEach((p, i) => { grid[Math.min(T - 1, Math.floor((i + 0.5) * T / H))] = p.id; });          // humans spread through the grid
  const drivers = shuffle([...Array(10).keys()]).slice(0, nb); let k = 0;
  for (let j = 0; j < T; j++) if (grid[j] == null) grid[j] = 'b' + drivers[k++];
  r.phase = 'race'; r.startAt = now() + COUNTDOWN_MS; r.bots = [];
  for (const p of ps) { p.fin = null; p.lap = 0; p.best = 0; p.st = null; p.lastSt = 0; }
  broadcast(r, { t: 'start', startAt: r.startAt, settings: r.settings, grid, nb });
  r.timers.push(setTimeout(() => sendResults(r), COUNTDOWN_MS + RACE_TIMEOUT_MS));
  broadcastRoom(r);
}

/* ---------- message handling ---------- */
const isNums = (a, n) => Array.isArray(a) && a.length === n && a.every(v => typeof v === 'number' && Number.isFinite(v));
function handle(p, m) {
  const r = p.room, host = r && r.host === p.id;
  switch (m.t) {
    case 'hello': p.name = clean(m.name, p.name); p.team = int(m.team, 0, 7, 0); break;
    case 'ping': p.ping = int(m.r, 0, 2000, 0); send(p, { t: 'pong', c: m.c, s: now() }); break;
    case 'create': p.name = clean(m.name, p.name); p.team = int(m.team, 0, 7, p.team); { const rm = createRoom(p); broadcastRoom(rm); } break;
    case 'join': {
      p.name = clean(m.name, p.name); p.team = int(m.team, 0, 7, p.team);
      const rm = rooms.get(String(m.code || '').toUpperCase().trim());
      if (!rm) return send(p, { t: 'err', c: 'nocode' });
      if (rm === p.room) return broadcastRoom(rm);
      if (rm.phase !== 'lobby') return send(p, { t: 'err', c: 'inprogress' });
      if (rm.players.size >= MAX_PLAYERS) return send(p, { t: 'err', c: 'full' });
      leaveRoom(p); addPlayer(rm, p); broadcastRoom(rm); break;
    }
    case 'leave': leaveRoom(p); break;
    case 'ready': if (r && r.phase === 'lobby' && !host) { p.ready = !!m.ready; broadcastRoom(r); } break;
    case 'team': if (r && r.phase === 'lobby') { p.team = int(m.team, 0, 7, p.team); broadcastRoom(r); } break;
    case 'settings': {
      if (!r || r.phase !== 'lobby') return;
      if (!host) return send(p, { t: 'err', c: 'host' });
      const s = r.settings;
      if ('track' in m) s.track = int(m.track, 0, 4, s.track);
      if ('laps' in m) s.laps = LAPS.includes(+m.laps) ? +m.laps : s.laps;
      if ('assists' in m) s.assists = int(m.assists, 0, 2, s.assists);
      if ('bots' in m) s.bots = !!m.bots;
      if ('diff' in m) s.diff = int(m.diff, 0, 4, s.diff);
      for (const q of r.players.values()) if (q.id !== r.host) q.ready = false;     // settings changed: everyone confirms again
      broadcastRoom(r); break;
    }
    case 'start': {
      if (!r || r.phase !== 'lobby') return;
      if (!host) return send(p, { t: 'err', c: 'host' });
      if (![...r.players.values()].every(q => q.id === r.host || q.ready)) return send(p, { t: 'err', c: 'notready' });
      startRace(r); break;
    }
    case 'st': {                                   // car state at ~20 Hz: [x,z,h,vx,vz,vl,w,thr,brk,dl,idx,off,lap,rpm,flags]
      if (!r || r.phase !== 'race' || !isNums(m.s, 15)) return;
      const s = m.s, t = now();
      if (Math.abs(s[0]) > 6000 || Math.abs(s[1]) > 6000 || Math.abs(s[5]) > 140) return;
      if (p.st && !(s[14] & 16)) {                 // plausibility: no teleporting (flag 16 = driver pressed reset)
        const dt = Math.max(0.02, (t - p.lastSt) / 1000), d = Math.hypot(s[0] - p.st[0], s[1] - p.st[1]);
        if (d > 130 * dt + 12) return send(p, { t: 'corr', x: p.st[0], z: p.st[1], h: p.st[2], vx: p.st[3], vz: p.st[4] });
      }
      p.st = s; p.lastSt = t; p.lap = s[12];
      if (host && Array.isArray(m.b) && m.b.length <= 10 && m.b.every(b => Array.isArray(b) && b.length === 16 && typeof b[0] === 'string')) r.bots = m.b;   // host simulates the bots
      break;
    }
    case 'lap': if (r && r.phase === 'race') { const t = +m.lt; if (t > 5 && t < 600 && (!p.best || t < p.best)) p.best = t; } break;
    case 'fin': {
      if (!r || r.phase !== 'race' || p.fin != null) return;
      p.fin = Math.max(0.1, (now() - p.ping / 2 - r.startAt) / 1000);                 // authoritative finish time = server clock - half RTT
      broadcast(r, { t: 'fin', id: p.id, t2: p.fin, pos: [...r.players.values()].filter(q => q.fin != null).length });
      if (!r.timers.fin) r.timers.fin = r.timers.push(setTimeout(() => sendResults(r), FINISH_GRACE_MS));
      checkFinish(r); break;
    }
    default: break;
  }
}

/* ---------- http + websocket ---------- */
const clientFile = ['CLIENT_FILE' in process.env ? process.env.CLIENT_FILE : '', 'index.html', 'jpedg3d_fase7.html']
  .filter(Boolean).map(f => path.resolve(__dirname, f)).find(f => fs.existsSync(f));
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ ok: true, rooms: rooms.size })); }
  if ((req.url === '/' || req.url.startsWith('/?')) && clientFile) { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); return fs.createReadStream(clientFile).pipe(res); }
  res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('JPEDG3D server OK\n');
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024, perMessageDeflate: false });
wss.on('connection', ws => {
  const p = { id: 'u' + (++uid).toString(36), ws, name: 'PLAYER', team: 0, ready: false, ping: 0, room: null, alive: true, seen: now(), cnt: 0, cntT: now(), st: null, lastSt: 0 };
  ws.on('pong', () => { p.alive = true; });
  ws.on('message', data => {
    const t = now(); p.seen = t;
    if (t - p.cntT > 1000) { p.cntT = t; p.cnt = 0; }
    if (++p.cnt > MAX_MSG_PER_SEC) { if (p.cnt === MAX_MSG_PER_SEC + 1) send(p, { t: 'err', c: 'rate' }); return; }
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (m && typeof m.t === 'string') { try { handle(p, m); } catch (e) { console.error('handler error', e); } }
  });
  const gone = () => { leaveRoom(p, 'disconnect'); };
  ws.on('close', gone); ws.on('error', gone);
  send(p, { t: 'welcome', id: p.id, ts: now(), max: MAX_PLAYERS });
});

/* 20 Hz state relay + heartbeat / idle cleanup */
setInterval(() => {
  for (const r of rooms.values()) {
    if (r.phase !== 'race') continue;
    const all = [...r.players.values()].filter(p => p.st).map(p => [p.id, ...p.st]), ts = now();
    for (const p of r.players.values()) {
      const others = all.filter(e => e[0] !== p.id);
      if (others.length || (r.bots.length && r.host !== p.id)) send(p, { t: 'snap', ts, p: others, b: r.host === p.id ? undefined : r.bots });
    }
  }
}, TICK_MS);
setInterval(() => {
  for (const ws of wss.clients) { /* ws-level keepalive */ }
  for (const r of rooms.values()) for (const p of [...r.players.values()]) {
    if (!p.alive || now() - p.seen > IDLE_MS) { try { p.ws.terminate(); } catch (e) { /* ignore */ } leaveRoom(p, 'timeout'); }
    else { p.alive = false; try { p.ws.ping(); } catch (e) { /* ignore */ } }
  }
}, 10000);
setInterval(() => { for (const r of rooms.values()) if (r.phase === 'lobby') broadcastRoom(r); }, 3000);   // refresh pings in the lobby

server.listen(PORT, () => console.log(`JPEDG3D server listening on :${PORT}` + (clientFile ? `  (serving ${path.basename(clientFile)})` : '')));
module.exports = { server, rooms };
