const test = require('node:test');
const assert = require('node:assert');
const WebSocket = require('ws');
const server = require('./server');

const now = () => performance.timeOrigin + performance.now();
const sleep = ms => new Promise(r => setTimeout(r, ms));

// Test client; `lag` delays replies to server pings to simulate a slow network.
function client(url, lag = 0) {
  const ws = new WebSocket(url), inbox = [], waiters = [];
  ws.on('message', raw => {
    const m = JSON.parse(raw);
    if (m.t === 'sping') return setTimeout(() => ws.send(JSON.stringify({ t: 'spong', s: m.s })), lag);
    const i = waiters.findIndex(w => w.t === m.t);
    i >= 0 ? waiters.splice(i, 1)[0].resolve(m) : inbox.push(m);
  });
  return Object.assign(ws, {
    ready: new Promise(r => ws.on('open', r)),
    out: m => ws.send(JSON.stringify(m)),
    next: t => {
      const i = inbox.findIndex(m => m.t === t);
      return i >= 0 ? Promise.resolve(inbox.splice(i, 1)[0]) : new Promise(resolve => waiters.push({ t, resolve }));
    },
  });
}

test('buzz order, reconnect, rename, score, end', async () => {
  await new Promise(r => server.listen(0, r));
  const url = 'ws://localhost:' + server.address().port;
  const host = client(url), fast = client(url), slow = client(url, 150), cheat = client(url);
  await Promise.all([host, fast, slow, cheat].map(c => c.ready));
  let again;
  try {

  host.out({ t: 'create' });
  const { code, hostToken } = await host.next('created');
  host.out({ t: 'host', code, hostToken });
  fast.out({ t: 'join', code, name: 'Fast' });
  slow.out({ t: 'join', code, name: 'Slow' });
  cheat.out({ t: 'join', code, name: 'Cheat' });
  const ids = { fast: (await fast.next('joined')).playerId, slow: (await slow.next('joined')).playerId, cheat: (await cheat.next('joined')).playerId };
  await sleep(200); // let the server measure each client's round trip

  host.out({ t: 'open' });
  await sleep(50);
  const pressed = now();
  fast.out({ t: 'buzz', at: pressed });
  await sleep(30);
  cheat.out({ t: 'buzz', at: pressed - 100000 }); // claims a press long before it could have happened
  await sleep(40); // slow player pressed 20ms before fast, but their message arrives ~70ms later
  slow.out({ t: 'buzz', at: pressed - 20 });
  await sleep(50);

  const states = [];
  host.on('message', raw => { const m = JSON.parse(raw); if (m.t === 'state') states.push(m); });
  // Rejoin as the slow player on a fresh socket (simulates a refresh) and rename.
  slow.close();
  again = client(url);
  await again.ready;
  again.out({ t: 'join', code, playerId: ids.slow });
  assert.equal((await again.next('joined')).playerId, ids.slow);
  again.out({ t: 'rename', name: 'Renamed' });
  host.out({ t: 'score', playerId: ids.fast, delta: 5 });
  host.out({ t: 'score', playerId: ids.fast, delta: -2 });
  await sleep(100);

  const s = states.at(-1);
  assert.deepEqual(s.buzzes.map(b => b.id), [ids.slow, ids.fast, ids.cheat], 'earlier real press wins; cheat clamped to within one round trip of arrival');
  assert.ok(Math.abs(s.buzzes[1].ms - s.buzzes[0].ms - 20) < 1, 'gap between slow and fast is the real 20ms');
  assert.equal(s.players.length, 3, 'refresh did not create a duplicate player');
  assert.equal(s.players.find(p => p.id === ids.slow).name, 'Renamed');
  assert.equal(s.players.find(p => p.id === ids.fast).score, 3);

  host.out({ t: 'end' });
  await again.next('ended');
  again.out({ t: 'join', code, playerId: ids.slow });
  await again.next('ended'); // room is gone after the host ends it
  } finally {
    for (const c of [host, fast, slow, cheat, again]) c?.close();
    await new Promise(r => server.close(r));
  }
});
