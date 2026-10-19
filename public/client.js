// Shared by host and player pages: auto-reconnecting socket + server clock sync.
const clientNow = () => performance.timeOrigin + performance.now();
const $ = id => document.getElementById(id);
const esc = s => String(s).replace(/[&<>"']/g, c => `&#${c.charCodeAt(0)};`);

function connect(onOpen, onMessage) {
  let ws, offset = 0, best = Infinity;
  const api = {
    send: m => ws?.readyState === 1 && ws.send(JSON.stringify(m)),
    offset: () => offset,
  };
  // NTP-style: keep the offset from the sample with the smallest round trip.
  function sync() {
    best = Infinity;
    for (let i = 0; i < 8; i++) setTimeout(() => api.send({ t: 'ping', c: clientNow() }), i * 100);
  }
  function open() {
    ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host);
    ws.onopen = () => { document.body.dataset.conn = 'on'; sync(); onOpen(api); };
    ws.onmessage = e => {
      const m = JSON.parse(e.data);
      if (m.t === 'pong') {
        const c1 = clientNow(), rtt = c1 - m.c;
        if (rtt < best) { best = rtt; offset = m.s - (m.c + c1) / 2; }
      } else if (m.t === 'sping') api.send({ t: 'spong', s: m.s });
      else onMessage(m, api);
    };
    ws.onclose = () => { document.body.dataset.conn = 'off'; setTimeout(open, 1000); };
  }
  setInterval(sync, 30000);
  document.addEventListener('visibilitychange', () => document.visibilityState === 'visible' && sync());
  open();
  return api;
}

function renderBuzzes(s) {
  const name = id => esc(s.players.find(p => p.id === id)?.name ?? '?');
  if (!s.buzzes.length) return `<li class="empty muted">${s.open ? 'Waiting for buzzes…' : 'Buzzers are locked'}</li>`;
  return s.buzzes.map((b, i) => `<li><b>${name(b.id)}</b><span class="time">${
    i === 0 ? `${b.ms.toFixed(1)} ms` : `+${(b.ms - s.buzzes[0].ms).toFixed(1)} ms`}</span></li>`).join('');
}
