/* FlowScope — market-data client and ten-second pressure observation. */
(() => {
  'use strict';

  const $ = id => document.getElementById(id);
  const venues = ['Binance', 'Coinbase', 'Kraken', 'OKX', 'Bybit', 'Gate.io', 'Bitstamp', 'Bitget'];
  const storageKey = 'flowscope.verified-pressure.v1';
  const formatPrice = n => Number(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: n < 10 ? 5 : 2 });
  const money = n => n >= 1e9 ? `$${(n / 1e9).toFixed(2)}B` : n >= 1e6 ? `$${(n / 1e6).toFixed(2)}M` : n >= 1e3 ? `$${(n / 1e3).toFixed(1)}K` : `$${n.toFixed(2)}`;
  const clock = time => new Date(time).toLocaleTimeString('en-GB', { hour12: false });
  const percent = n => n == null ? '—' : `${n.toFixed(1)}%`;

  const state = {
    symbol: 'BTC', window: 60, generation: 0, feeds: new Map(), seconds: new Map(),
    recent: [], last: null, total: 0, startTime: Date.now(), lastVerified: null,
    pending: [], completed: [], lastSampleAt: 0, lastPredictionAt: 0,
    ema: null,
    streaks: { BUY: newStreak(), SELL: newStreak() },
    priceTrace: [], lastSnapshot: null
  };
  function newStreak() { return { base: null, started: 0, candidate: 0, falling: 0 }; }

  function loadHistory() {
    try {
      const data = JSON.parse(localStorage.getItem(storageKey) || '[]');
      state.completed = Array.isArray(data) ? data.filter(x => x && Number.isFinite(x.at) && ['BUY', 'SELL'].includes(x.side)).slice(-600) : [];
    } catch { state.completed = []; }
  }
  function persistHistory() {
    try { localStorage.setItem(storageKey, JSON.stringify(state.completed.slice(-600))); } catch { /* Private browser mode may block storage. */ }
  }

  function accept(name, side, price, qty, timestamp, tradeId) {
    const f = state.feeds.get(name), p = Number(price), q = Number(qty), t = Number(timestamp) || Date.now();
    side = String(side).toUpperCase();
    if (!f || !['BUY', 'SELL'].includes(side) || !Number.isFinite(p * q) || p <= 0 || q <= 0 || Math.abs(Date.now() - t) > 90000) return;
    if (tradeId !== undefined && tradeId !== null && tradeId !== '') {
      const id = `${name}:${state.symbol}:${tradeId}`;
      if (f.ids.has(id)) return;
      f.ids.add(id);
      if (f.ids.size > 10000) f.ids.clear();
    }
    const record = { name, side, price: p, value: p * q, t };
    state.recent.unshift(record);
    if (state.recent.length > 6000) state.recent.length = 6000;
    state.priceTrace.push(record);
    if (state.priceTrace.length > 120) state.priceTrace.shift();
    state.last = record;
    state.total++;
    f.received++;
    f.lastTradeAt = Date.now();
    f.status = 'receiving';
    const key = Math.floor(t / 1000);
    let bucket = state.seconds.get(key);
    if (!bucket) {
      bucket = { buy: 0, sell: 0, count: 0, venues: new Set() };
      state.seconds.set(key, bucket);
    }
    bucket[side === 'BUY' ? 'buy' : 'sell'] += p * q;
    bucket.count++;
    bucket.venues.add(name);
  }

  function settings(name) {
    const asset = state.symbol, usdt = asset + 'USDT';
    switch (name) {
      case 'Binance': return {
        url: `wss://data-stream.binance.vision/ws/${usdt.toLowerCase()}@trade`,
        parse: d => { if (d.e === 'trade') accept(name, d.m ? 'SELL' : 'BUY', d.p, d.q, d.T, d.t); }
      };
      case 'Coinbase': return {
        url: 'wss://ws-feed.exchange.coinbase.com',
        subscribe: { type: 'subscribe', product_ids: [`${asset}-USD`], channels: ['matches'] },
        parse: d => { if (d.type === 'match') accept(name, d.side === 'buy' ? 'SELL' : 'BUY', d.price, d.size, Date.parse(d.time), d.trade_id); }
      };
      case 'Kraken': return {
        url: 'wss://ws.kraken.com/v2',
        subscribe: { method: 'subscribe', params: { channel: 'trade', symbol: [`${asset}/USD`] } },
        parse: d => { if (d.channel === 'trade' && Array.isArray(d.data)) d.data.forEach(x => accept(name, x.side, x.price, x.qty, Date.parse(x.timestamp), x.trade_id)); }
      };
      case 'OKX': return {
        url: 'wss://ws.okx.com:8443/ws/v5/public',
        subscribe: { op: 'subscribe', args: [{ channel: 'trades', instId: `${asset}-USDT` }] },
        parse: d => { if (d.arg?.channel === 'trades' && Array.isArray(d.data)) d.data.forEach(x => accept(name, x.side, x.px, x.sz, x.ts, x.tradeId)); }
      };
      case 'Bybit': return {
        url: 'wss://stream.bybit.com/v5/public/spot',
        subscribe: { op: 'subscribe', args: [`publicTrade.${usdt}`] },
        ping: JSON.stringify({ op: 'ping' }),
        parse: d => { if (d.topic?.startsWith('publicTrade.') && Array.isArray(d.data)) d.data.forEach(x => accept(name, x.S, x.p, x.v, x.T, x.i)); }
      };
      case 'Gate.io': return {
        url: 'wss://api.gateio.ws/ws/v4/',
        subscribe: { time: Math.floor(Date.now() / 1000), channel: 'spot.trades', event: 'subscribe', payload: [`${asset}_USDT`] },
        parse: d => {
          if (d.channel !== 'spot.trades' || d.event !== 'update') return;
          for (const x of Array.isArray(d.result) ? d.result : [d.result]) {
            if (x) accept(name, x.side, x.price, x.amount, Number(x.create_time_ms) || Number(x.create_time) * 1000, x.id);
          }
        }
      };
      case 'Bitstamp': return {
        url: 'wss://ws.bitstamp.net',
        subscribe: { event: 'bts:subscribe', data: { channel: `live_trades_${asset.toLowerCase()}usd` } },
        parse: d => {
          if (d.event !== 'trade' || !d.data) return;
          const x = d.data;
          accept(name, Number(x.type) === 0 ? 'BUY' : Number(x.type) === 1 ? 'SELL' : 'UNKNOWN', x.price, x.amount, Number(x.microtimestamp) ? Number(x.microtimestamp) / 1000 : Number(x.timestamp) * 1000, x.id);
        }
      };
      case 'Bitget': return {
        url: 'wss://ws.bitget.com/v2/ws/public',
        subscribe: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'trade', instId: usdt }] },
        ping: 'ping',
        parse: d => {
          if (d.arg?.channel !== 'trade' || !Array.isArray(d.data)) return;
          for (const x of d.data) {
            if (Array.isArray(x)) accept(name, x[3], x[1], x[2], x[0], x[4]);
            else accept(name, x.side, x.price ?? x.p, x.size ?? x.sz, x.ts, x.tradeId ?? x.id);
          }
        }
      };
    }
  }

  function disconnect(feed) {
    if (feed?.retryTimer) clearTimeout(feed.retryTimer);
    if (feed) feed.retryTimer = null;
    if (feed?.socket) {
      const socket = feed.socket;
      feed.socket = null;
      socket.onopen = socket.onmessage = socket.onerror = socket.onclose = null;
      try { socket.close(); } catch { /* Already disconnected. */ }
    }
  }
  function scheduleRetry(name, reason) {
    const f = state.feeds.get(name);
    if (!f || f.disabled) return;
    disconnect(f);
    f.status = 'retrying';
    f.reason = reason;
    f.retries++;
    const wait = Math.min(30000, 1000 * 2 ** Math.min(f.retries, 5)) + Math.random() * 500;
    f.retryTimer = setTimeout(() => { f.retryTimer = null; connect(name); }, wait);
  }
  function connect(name) {
    const f = state.feeds.get(name);
    if (!f || f.disabled) return;
    disconnect(f);
    f.status = 'connecting';
    f.openingAt = f.lastMessageAt = Date.now();
    const generation = state.generation;
    try {
      const socket = new WebSocket(f.settings.url);
      f.socket = socket;
      socket.onopen = () => {
        if (state.generation !== generation || f.socket !== socket) return;
        f.status = 'connected';
        f.retries = 0;
        f.lastMessageAt = Date.now();
        if (f.settings.subscribe) socket.send(JSON.stringify(f.settings.subscribe));
      };
      socket.onmessage = event => {
        if (state.generation !== generation || f.socket !== socket) return;
        f.lastMessageAt = Date.now();
        try {
          const message = JSON.parse(event.data);
          if (message.event === 'error' || message.success === false) {
            scheduleRetry(name, 'Subscription rejected');
            return;
          }
          f.settings.parse(message);
        } catch { f.reason = 'Unrecognized message'; }
      };
      socket.onerror = () => { if (f.socket === socket) scheduleRetry(name, 'Network error'); };
      socket.onclose = () => { if (f.socket === socket) scheduleRetry(name, 'Disconnected'); };
    } catch { scheduleRetry(name, 'WebSocket unavailable'); }
  }
  function resetTrend() {
    state.ema = null;
    state.streaks = { BUY: newStreak(), SELL: newStreak() };
    state.pending = [];
    state.lastPredictionAt = 0;
  }
  function startSymbol() {
    state.generation++;
    for (const f of state.feeds.values()) disconnect(f);
    state.feeds.clear();
    state.seconds.clear();
    state.recent = [];
    state.priceTrace = [];
    state.last = null;
    state.total = 0;
    state.startTime = Date.now();
    resetTrend();
    for (const name of venues) {
      const disabled = state.symbol === 'PAXG' && ['Bybit', 'Bitstamp', 'Bitget'].includes(name);
      state.feeds.set(name, {
        name, disabled, settings: settings(name), socket: null, retryTimer: null,
        retries: 0, received: 0, lastTradeAt: 0, lastMessageAt: 0, openingAt: 0,
        status: disabled ? 'unavailable' : 'connecting', reason: '', ids: new Set()
      });
    }
    for (const name of venues) if (!state.feeds.get(name).disabled) connect(name);
    $('goldNotice').hidden = state.symbol !== 'PAXG';
    render();
  }
  function watchdog() {
    const now = Date.now();
    for (const name of venues) {
      const f = state.feeds.get(name);
      if (!f || f.disabled) continue;
      const s = f.socket;
      if (s?.readyState === WebSocket.CONNECTING && now - f.openingAt > 16000) scheduleRetry(name, 'Handshake timed out');
      else if (s?.readyState === WebSocket.OPEN && now - f.lastMessageAt > 85000) scheduleRetry(name, 'Feed stopped responding');
      else if (!s && !f.retryTimer) scheduleRetry(name, 'Connection unavailable');
      else if (s?.readyState === WebSocket.OPEN && f.settings.ping && now - f.lastMessageAt > 18000) {
        try { s.send(f.settings.ping); } catch { scheduleRetry(name, 'Keepalive failed'); }
      }
    }
  }

  function summarize(duration) {
    const to = Math.floor(Date.now() / 1000), from = to - duration;
    let buy = 0, sell = 0, count = 0;
    const active = new Set();
    for (let second = from; second <= to; second++) {
      const bucket = state.seconds.get(second);
      if (!bucket) continue;
      buy += bucket.buy;
      sell += bucket.sell;
      count += bucket.count;
      for (const name of bucket.venues) active.add(name);
    }
    return { buy, sell, count, venues: active.size, ratio: buy + sell ? 100 * buy / (buy + sell) : null };
  }
  function updateStreaks(ratio, valid, now) {
    if (!valid || ratio == null) { resetTrend(); return; }
    state.ema = state.ema == null ? ratio : .78 * state.ema + .22 * ratio;
    for (const side of ['BUY', 'SELL']) {
      const x = state.streaks[side], value = side === 'BUY' ? state.ema : 100 - state.ema;
      if (x.base == null) x.base = value;
      if (!x.started) {
        if (value >= x.base + .7) {
          x.candidate ||= now;
          if (now - x.candidate >= 2500) { x.started = x.candidate; x.base = value; x.falling = 0; }
        } else if (value < x.base - 1.2) { x.base = value; x.candidate = 0; }
        else x.candidate = 0;
      } else if (value <= x.base - 1.4) {
        x.falling ||= now;
        if (now - x.falling >= 2500) Object.assign(x, newStreak(), { base: value });
      } else { x.falling = 0; if (value > x.base) x.base = value; }
    }
  }
  function timeString(start) {
    if (!start) return '00:00';
    const elapsed = Math.max(0, Math.floor((Date.now() - start) / 1000));
    return `${String(Math.floor(elapsed / 60)).padStart(2, '0')}:${String(elapsed % 60).padStart(2, '0')}`;
  }
  function feature(side, short, long) {
    const value = side === 'BUY' ? short.ratio : 100 - short.ratio;
    const reference = side === 'BUY' ? long.ratio : 100 - long.ratio;
    const streak = state.streaks[side].started ? (Date.now() - state.streaks[side].started) / 1000 : 0;
    return { symbol: state.symbol, side,
      duration: streak >= 30 ? 'long' : streak >= 10 ? 'medium' : 'short',
      strength: value >= 60 ? 'high' : value >= 52 ? 'medium' : 'low',
      slope: value - reference >= 3 ? 'up' : value - reference <= -3 ? 'down' : 'flat' };
  }
  function sameFeature(a, b) {
    return a.symbol === b.symbol && a.side === b.side && a.duration === b.duration && a.strength === b.strength && a.slope === b.slope;
  }
  function assessForecasts(short, long, healthy) {
    const now = Date.now();
    for (let i = state.pending.length - 1; i >= 0; i--) {
      const item = state.pending[i];
      if (now - item.at < 10000) continue;
      state.pending.splice(i, 1);
      if (!healthy || short.ratio == null || now - item.at > 14500) continue;
      const latest = item.side === 'BUY' ? short.ratio : 100 - short.ratio;
      const change = latest - item.from;
      const result = change >= .7 ? 'CONTINUED' : change <= -.7 ? 'REVERSED' : 'FLAT';
      const checked = { ...item, change, result, checkedAt: now };
      state.completed.push(checked);
      state.completed = state.completed.slice(-600);
      state.lastVerified = checked;
      persistHistory();
    }
    if (!healthy || short.ratio == null || long.ratio == null || now - state.lastPredictionAt < 5000) return;
    state.lastPredictionAt = now;
    for (const side of ['BUY', 'SELL']) {
      state.pending.push({ ...feature(side, short, long), at: now, from: side === 'BUY' ? short.ratio : 100 - short.ratio });
    }
  }
  function showOutlook(side, short, long, healthy) {
    const prefix = side === 'BUY' ? 'buy' : 'sell';
    let heading = 'INSUFFICIENT DATA', reason = 'Waiting for sufficient live coverage', probability = 'Not calibrated', width = 0;
    if (healthy && short.ratio != null && long.ratio != null) {
      const matches = state.completed.filter(x => sameFeature(x, feature(side, short, long)));
      if (matches.length < 20) {
        heading = state.streaks[side].started ? 'ACTIVE · UNVERIFIED' : 'NO CONFIRMED STREAK';
        reason = `${matches.length}/20 comparable verified outcomes collected`;
      } else {
        const successes = matches.filter(x => x.result === 'CONTINUED').length;
        const rate = 100 * (successes + 1) / (matches.length + 2);
        heading = rate >= 60 && state.streaks[side].started ? 'LIKELY CONTINUE' : rate <= 40 ? 'REVERSAL RISK' : 'UNCERTAIN';
        reason = `${matches.length} comparable ten-second checks`;
        probability = `${rate.toFixed(1)}% observed continuation estimate`;
        width = rate;
      }
    }
    $(`${prefix}Outlook`).textContent = heading;
    $(`${prefix}Reason`).textContent = reason;
    $(`${prefix}Probability`).textContent = probability;
    $(`${prefix}Likelihood`).style.width = `${width}%`;
  }
  function showStreak(side) {
    const prefix = side === 'BUY' ? 'buy' : 'sell';
    const start = state.streaks[side].started;
    const sec = start ? Math.max(0, (Date.now() - start) / 1000) : 0;
    const level = !start ? 0 : sec < 10 ? 1 : sec < 30 ? 2 : sec < 60 ? 3 : 4;
    $(`${prefix}Card`).dataset.strength = String(level);
    $(`${prefix}Timer`).textContent = timeString(start);
    $(`${prefix}TimerState`).textContent = start ? 'Confirmed increasing pressure' : 'Awaiting confirmed increase';
    $(`${prefix}Strength`).style.width = `${start ? Math.min(100, sec / 180 * 100) : 0}%`;
    $(`${prefix}StrengthName`).textContent = ['IDLE', 'BUILDING', 'DEVELOPING', 'STRONG', 'SUSTAINED'][level];
  }

  function render() {
    const now = Date.now();
    for (const second of state.seconds.keys()) if (second < Math.floor((now - 420000) / 1000)) state.seconds.delete(second);
    const summary = summarize(state.window), short = summarize(30), long = summarize(300);
    const receiving = venues.filter(name => { const f = state.feeds.get(name); return f?.lastTradeAt && now - f.lastTradeAt < 15000; }).length;
    const connected = venues.filter(name => state.feeds.get(name)?.socket?.readyState === WebSocket.OPEN).length;
    const healthy = summary.count >= 30 && summary.venues >= 2 && receiving >= 2 && now - state.startTime >= 10000;
    updateStreaks(summary.ratio, healthy, now);
    assessForecasts(short, long, healthy);
    showStreak('BUY'); showStreak('SELL');
    showOutlook('BUY', short, long, healthy); showOutlook('SELL', short, long, healthy);

    $('indicator').className = `indicator${receiving ? ' online' : ''}`;
    $('connectionText').textContent = receiving ? `${receiving} RECEIVING · ${connected} CONNECTED` : connected ? `${connected} CONNECTED · WAITING` : 'RECONNECTING';
    $('feedCount').textContent = `${receiving} receiving · ${connected} connected`;
    $('exchangeRows').innerHTML = venues.map(name => {
      const f = state.feeds.get(name);
      if (!f) return '';
      const fresh = f.lastTradeAt && now - f.lastTradeAt < 15000;
      const status = f.disabled ? 'unavailable' : fresh ? 'receiving' : f.status === 'receiving' ? 'connected · quiet' : f.status;
      return `<div class="connection-row"><b><i class="${fresh ? 'receiving' : status}"></i>${name}</b><small>${status.toUpperCase()} · ${f.received.toLocaleString()} trades</small></div>`;
    }).join('');

    $('lastPrice').textContent = state.last ? `$${formatPrice(state.last.price)}` : '—';
    $('lastDetails').textContent = state.last ? `${state.last.name} · ${clock(state.last.t)}` : 'Waiting for valid executions';
    $('buyNotional').textContent = summary.count ? money(summary.buy) : '—';
    $('sellNotional').textContent = summary.count ? money(summary.sell) : '—';
    $('buyTrades').textContent = `${summary.count} total window prints`;
    $('sellTrades').textContent = `${summary.count} total window prints`;
    const delta = summary.buy - summary.sell;
    $('volumeDelta').textContent = summary.count ? `${delta >= 0 ? '+' : '−'}${money(Math.abs(delta))}` : '—';
    $('volumeDelta').className = delta >= 0 ? 'green' : 'red';
    $('coverageText').textContent = `${summary.count} prints / ${summary.venues} venues`;
    $('buyPercent').textContent = percent(summary.ratio);
    $('sellPercent').textContent = summary.ratio == null ? '—' : percent(100 - summary.ratio);
    $('buyMeter').style.width = `${summary.ratio ?? 50}%`;
    $('windowCaption').textContent = `Rolling ${state.window} seconds`;
    $('qualityNote').textContent = healthy ? 'Live-data requirements met. Noise filter: exponential smoothing, 0.7-point change threshold, 2.5-second confirmation. A longer trend is not proof of predictive accuracy.' : 'LOW COVERAGE · At least 30 prints, 2 receiving venues and 10 seconds of observations are needed.';

    $('totalTrades').textContent = `${state.total.toLocaleString()} received`;
    $('bufferCount').textContent = `${state.recent.length.toLocaleString()} recent prints in memory`;
    const venue = $('venueFilter').value, side = $('sideFilter').value, min = Math.max(0, Number($('minValue').value) || 0);
    const shown = state.recent.filter(x => (venue === 'ALL' || x.name === venue) && (side === 'ALL' || x.side === side) && x.value >= min).slice(0, 80);
    $('tradeRows').innerHTML = shown.length ? shown.map(x => `<tr><td>${clock(x.t)}</td><td>${x.name}</td><td class="${x.side === 'BUY' ? 'green' : 'red'}">${x.side}</td><td>$${formatPrice(x.price)}</td><td>${money(x.value)}</td></tr>`).join('') : '<tr><td colspan="5" class="empty">No matching real executions received yet</td></tr>';

    if (state.priceTrace.length > 1) {
      const values = state.priceTrace.map(x => x.price), lo = Math.min(...values), hi = Math.max(...values) || lo + 1, range = hi === lo ? 1 : hi - lo;
      $('pricePath').setAttribute('d', values.map((p, i) => `${i ? 'L' : 'M'}${(i * 500 / (values.length - 1)).toFixed(1)},${(120 - 110 * (p - lo) / range).toFixed(1)}`).join(' '));
      $('pricePlaceholder').hidden = true;
    } else { $('pricePath').setAttribute('d', ''); $('pricePlaceholder').hidden = false; }

    const checks = state.completed.filter(x => x.symbol === state.symbol);
    const directional = checks.filter(x => x.result !== 'FLAT');
    const successes = directional.filter(x => x.result === 'CONTINUED');
    $('verifiedCount').textContent = `${checks.length} completed checks`;
    $('lastOutcome').textContent = state.lastVerified ? `${state.lastVerified.side} ${state.lastVerified.result}` : '—';
    $('continuationRate').textContent = directional.length ? `${(successes.length / directional.length * 100).toFixed(1)}% (${directional.length} directional)` : '—';
    $('forecastTimestamp').textContent = state.lastPredictionAt ? clock(state.lastPredictionAt) : '—';
    $('historyCount').textContent = `${checks.length} outcomes`;
    $('historyRows').innerHTML = checks.length ? checks.slice(-12).reverse().map(x => `<tr><td>${clock(x.at)}</td><td>${x.symbol}</td><td>${x.side}</td><td>${x.result}</td><td>${x.change >= 0 ? '+' : ''}${x.change.toFixed(1)}pp</td></tr>`).join('') : '<tr><td colspan="5" class="empty">No completed ten-second checks yet</td></tr>';
  }

  document.querySelectorAll('[data-symbol]').forEach(button => button.addEventListener('click', () => {
    document.querySelectorAll('[data-symbol]').forEach(x => x.classList.remove('active'));
    button.classList.add('active');
    state.symbol = button.dataset.symbol;
    startSymbol();
  }));
  $('windowLength').addEventListener('change', event => { state.window = Number(event.target.value); resetTrend(); render(); });
  $('reconnect').addEventListener('click', () => { for (const name of venues) if (!state.feeds.get(name)?.disabled) connect(name); render(); });
  for (const id of ['venueFilter', 'sideFilter', 'minValue']) $(id).addEventListener(id === 'minValue' ? 'input' : 'change', render);
  $('clearHistory').addEventListener('click', () => { state.completed = state.completed.filter(x => x.symbol !== state.symbol); persistHistory(); render(); });
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) state.pending = []; // Never verify through a known paused period.
    else { watchdog(); render(); }
  });
  window.addEventListener('online', () => { watchdog(); render(); });
  loadHistory();
  startSymbol();
  setInterval(render, 1000);
  setInterval(watchdog, 7000);
})();