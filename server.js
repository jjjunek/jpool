// 더제이풀 서버: 추가 설치 없이 Node.js만으로 동작합니다.
// 1) 웹페이지(public/index.html)를 보여주고  2) /ws 로 접속한 사람들의 위치·모습을 서로에게 전달합니다.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 80;                                   // 동시 접속 최대 인원
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const page = path.join(__dirname, 'public', 'index.html');

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.end('ok'); return; }
  if (req.url === '/' || req.url.startsWith('/?') || req.url === '/index.html') {
    fs.readFile(page, (e, d) => {
      if (e) { res.writeHead(500); res.end('public/index.html 파일이 없어요'); return; }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
      res.end(d);
    });
    return;
  }
  res.writeHead(404); res.end('not found');
});

const clients = new Map();
let nextId = 1, dirty = false;

function frame(op, buf) {
  const n = buf.length; let h;
  if (n < 126) h = Buffer.from([0x80 | op, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, buf]);
}
const sendText = (c, s) => { if (!c.socket.destroyed) c.socket.write(frame(1, Buffer.from(s))); };

server.on('upgrade', (req, socket) => {
  if (req.url !== '/ws' || clients.size >= MAX_PLAYERS || !req.headers['sec-websocket-key']) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  const c = { id: 'p' + (nextId++), socket, buf: Buffer.alloc(0), p: null, count: 0, win: Date.now() };
  clients.set(c.id, c);
  sendText(c, JSON.stringify({ t: 'hello', id: c.id }));
  dirty = true;
  socket.on('data', d => {
    c.buf = Buffer.concat([c.buf, d]);
    if (c.buf.length > 16384) { socket.destroy(); return; }
    parse(c);
  });
  const drop = () => { if (clients.delete(c.id)) dirty = true; };
  socket.on('close', drop); socket.on('error', drop); socket.on('end', drop);
});

function parse(c) {
  for (;;) {
    const b = c.buf; if (b.length < 2) return;
    const op = b[0] & 15; let len = b[1] & 127, off = 2; const masked = !!(b[1] & 128);
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; const big = b.readBigUInt64BE(2); if (big > 8192n) { c.socket.destroy(); return; } len = Number(big); off = 10; }
    if (!masked || len > 4096) { c.socket.destroy(); return; }
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4), data = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
    c.buf = b.subarray(off + 4 + len);
    if (op === 8) { c.socket.end(frame(8, Buffer.alloc(0))); return; }
    if (op === 9) c.socket.write(frame(10, data));
    else if (op === 1) onMsg(c, data.toString());
  }
}

function onMsg(c, s) {
  const now = Date.now();
  if (now - c.win > 1000) { c.win = now; c.count = 0; }
  if (++c.count > 30) return;                              // 1초에 30번 넘게 보내면 무시
  let o; try { o = JSON.parse(s); } catch (e) { return; }
  if (o && typeof o === 'object' && !Array.isArray(o)) { c.p = o; dirty = true; }
}

// 0.1초마다, 바뀐 게 있으면 모두에게 "지금 접속한 사람들의 상태"를 한꺼번에 보냄
setInterval(() => {
  if (!dirty) return; dirty = false;
  const peers = []; clients.forEach(c => { if (c.p) peers.push({ peer: c.id, presence: c.p }); });
  const msg = JSON.stringify({ t: 'peers', peers });
  clients.forEach(c => sendText(c, msg));
}, 100);
// 연결이 끊기지 않게 30초마다 신호 보내기
setInterval(() => clients.forEach(c => { if (!c.socket.destroyed) c.socket.write(frame(9, Buffer.alloc(0))); }), 30000);

server.listen(PORT, () => console.log('더제이풀 서버 실행 중: http://localhost:' + PORT));
