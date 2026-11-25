/* Market calculations are separated from the exchange adapters so they can be tested without a live feed. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.FlowEngine = api;
})(typeof globalThis === 'object' ? globalThis : this, function () {
  'use strict';
  const MIN_NOTIONAL = 0.000001;
  const clamp = (x, a, b) => Math.min(b, Math.max(a, x));
  const pct = (a, b) => a + b > 0 ? (100 * a / (a + b)) : null;

  function normalize(raw, now = Date.now()) {
    if (!raw || !raw.venue || !raw.asset || !raw.quote) return null;
    const price = Number(raw.price), quantity = Number(raw.quantity), time = Number(raw.time);
    const side = String(raw.side || '').toUpperCase();
    if (!Number.isFinite(price * quantity) || price <= 0 || quantity <= 0 || price * quantity < MIN_NOTIONAL) return null;
    if (!Number.isFinite(time) || time < now - 120000 || time > now + 5000) return null;
    if (side !== 'BUY' && side !== 'SELL') return null;
    return { venue: raw.venue, asset: raw.asset, quote: raw.quote, price, quantity, value: price * quantity,
      side, time: Math.round(time), tradeId: raw.tradeId == null ? null : String(raw.tradeId), receivedAt: now };
  }
  function summary(trades, start, end) {
    let buy = 0, sell = 0, count = 0, first = null, last = null;
    const venues = new Map(), byVenue = new Map();
    for (const t of trades) {
      if (t.time < start || t.time >= end) continue;
      count++;
      if (t.side === 'BUY') buy += t.value; else sell += t.value;
      venues.set(t.venue, true);
      const v = byVenue.get(t.venue) || { buy: 0, sell: 0, count: 0 };
      v[t.side === 'BUY' ? 'buy' : 'sell'] += t.value; v.count++; byVenue.set(t.venue, v);
      if (!first || t.time < first.time) first = t;
      if (!last || t.time > last.time) last = t;
    }
    return { buy, sell, count, venueCount: venues.size, byVenue,
      buyPercent: pct(buy, sell), firstPrice: first?.price ?? null,
      lastPrice: last?.price ?? null, firstTime: first?.time ?? null, lastTime: last?.time ?? null };
  }
  // Use one-second buckets for pressure, rather than silently losing high-volume trades
  // when the on-screen tape is capped. Window boundaries are half-open [start, end).
  function summaryBins(bins, start, end) {
    let buy = 0, sell = 0, count = 0;
    const byVenue = new Map();
    for (let second = Math.ceil(start / 1000); second < Math.ceil(end / 1000); second++) {
      const bucket = bins.get(second);
      if (!bucket) continue;
      buy += bucket.buy; sell += bucket.sell; count += bucket.count;
      for (const [venue, v] of bucket.byVenue) {
        const current = byVenue.get(venue) || {buy:0,sell:0,count:0};
        current.buy += v.buy; current.sell += v.sell; current.count += v.count;
        byVenue.set(venue, current);
      }
    }
    return {buy,sell,count,venueCount:byVenue.size,byVenue,buyPercent:pct(buy,sell)};
  }
  function compare(a, b, deadband = 2) {
    if (a == null || b == null) return 'UNKNOWN';
    const d = b - a;
    if (d >= deadband) return 'BUY_RISING';
    if (d <= -deadband) return 'SELL_RISING';
    return 'FLAT';
  }
  function direction(share, threshold = 55, hysteresis = 2) {
    if (share == null) return 'UNKNOWN';
    if (share >= threshold + hysteresis) return 'BUY_DOMINANT';
    if (share <= 100 - threshold - hysteresis) return 'SELL_DOMINANT';
    return 'NEUTRAL';
  }
  function qualify(window, lastPacketAge, observationAge, opts = {}) {
    const minTrades = opts.minTrades ?? 40;
    const minVenues = opts.minVenues ?? 2;
    const minAge = opts.minAge ?? 10000;
    const reasons = [];
    if (window.count < minTrades) reasons.push('INSUFFICIENT_TRADES');
    if (window.venueCount < minVenues) reasons.push('INSUFFICIENT_VENUES');
    if (observationAge < minAge) reasons.push('WARMING_UP');
    if (lastPacketAge > 6000) reasons.push('STALE_TRADES');
    return { ok: reasons.length === 0, reasons };
  }
  function agreement(byVenue, side, minValue = 100) {
    let matched = 0, eligible = 0;
    for (const v of byVenue.values()) {
      if (v.buy + v.sell < minValue) continue;
      eligible++;
      if ((side === 'BUY' && v.buy > v.sell) || (side === 'SELL' && v.sell > v.buy)) matched++;
    }
    return { matched, eligible, ratio: eligible ? matched / eligible : null };
  }
  function estimate(near, earlier, main, quality) {
    if (!quality.ok || near.buyPercent == null || earlier.buyPercent == null || main.buyPercent == null) {
      return { side: 'ABSTAIN', label: 'INSUFFICIENT DATA', reason: quality.reasons.join(', ') || 'Missing historical window' };
    }
    const diff = near.buyPercent - earlier.buyPercent;
    const slope = near.buyPercent - main.buyPercent;
    const side = diff >= 3 && slope >= 2 && near.buyPercent >= 55 ? 'BUY' :
      diff <= -3 && slope <= -2 && near.buyPercent <= 45 ? 'SELL' : 'ABSTAIN';
    if (side === 'ABSTAIN') return { side, label: 'NO CLEAR EDGE', reason: 'Pressure momentum and broader bias do not align' };
    const a = agreement(near.byVenue, side);
    if (a.eligible < 2 || a.ratio < .6) return { side: 'ABSTAIN', label: 'VENUE DISAGREEMENT', reason: 'Aggressor flow is not confirmed across venues' };
    return { side, label: side === 'BUY' ? 'BUY PRESSURE MAY PERSIST' : 'SELL PRESSURE MAY PERSIST',
      reason: `${diff > 0 ? '+' : ''}${diff.toFixed(1)}pp recent momentum · ${a.matched}/${a.eligible} venues agree`,
      features: { side, momentum: Math.abs(diff) >= 10 ? 'fast' : 'normal', dominance: side === 'BUY' ? near.buyPercent >= 65 : near.buyPercent <= 35 } };
  }
  function rateFor(history, features, minSamples = 30) {
    if (!features) return { count: 0, success: 0, rate: null };
    const eligible = history.filter(x => x.features && x.features.side === features.side &&
      x.features.momentum === features.momentum && x.features.dominance === features.dominance &&
      (x.result === 'CORRECT' || x.result === 'WRONG')); 
    const count = eligible.length;
    const success = eligible.filter(x => x.result === 'CORRECT').length;
    return { count, success, rate: count >= minSamples ? 100 * success / count : null };
  }
  function settle(forecast, forward, opts = {}) {
    const minForward = opts.minForward ?? 20;
    const minVenues = opts.minVenues ?? 2;
    if (!forecast || !forward || forward.count < minForward || forward.venueCount < minVenues || forward.buyPercent == null)
      return { result: 'INVALID', delta: null, reason: 'Future-window coverage was insufficient' };
    const change = forward.buyPercent - forecast.baseline;
    const actual = compare(forecast.baseline, forward.buyPercent, 2);
    const predicted = forecast.side === 'BUY' ? 'BUY_RISING' : 'SELL_RISING';
    return { result: actual === 'FLAT' ? 'FLAT' : actual === predicted ? 'CORRECT' : 'WRONG', delta: change, reason: actual };
  }

  function consensus(rows) {
    const qualified=rows.filter(x=>x.ready&&Number.isFinite(x.share));
    const b=qualified.filter(x=>x.direction==='BUY_DOMINANT').length,s=qualified.filter(x=>x.direction==='SELL_DOMINANT').length;
    const mean=qualified.length?qualified.reduce((a,x)=>a+x.share,0)/qualified.length:null;
    const opposite=b>0&&s>0;
    const alignment=qualified.length?100*Math.max(b,s)/qualified.length:null;
    const coherent=qualified.length>=2&&!opposite&&Math.max(b,s)>=2&&alignment>=70&&(b>s?mean>=57:s>b?mean<=43:false);
    return {status:qualified.length<2?'WARMUP':opposite?'CONFLICT':coherent?'ALIGNED':'MIXED',share:coherent?mean:null,mean,alignment,buys:b,sells:s,qualified:qualified.length,selected:rows.length,leader:coherent?(b>s?'BUY':'SELL'):'NONE'};
  }
  function concentration(summary) {
    if(!summary?.byVenue||!summary.buy&&!summary.sell)return null;
    const total=summary.buy+summary.sell;let max=0;
    for(const v of summary.byVenue.values()) max=Math.max(max,(v.buy||0)+(v.sell||0));
    return max/total;
  }
  function wilson(success,total) {
    if(total<1)return null;
    const z=1.96,p=success/total,d=1+z*z/total,c=(p+z*z/(2*total))/d;
    const radius=z*Math.sqrt(p*(1-p)/total+z*z/(4*total*total))/d;
    return {rate:100*p,low:100*Math.max(0,c-radius),high:100*Math.min(1,c+radius)};
  }
  function benchmark(records,field='flowResult'){
    const eligible=records.filter(x=>x[field]==='CORRECT'||x[field]==='WRONG');
    const hits=eligible.filter(x=>x[field]==='CORRECT').length;
    const base=eligible.filter(x=>x.flowActual==='BUY'||x.flowActual==='SELL');
    const dominant=base.filter(x=>x.flowActual==='BUY').length>=base.length/2?'BUY':'SELL';
    return {total:eligible.length,hits,interval:wilson(hits,eligible.length),baseline:base.length?100*base.filter(x=>x.flowActual===dominant).length/base.length:null};
  }
  function displayTime(time, tz) {
    if (!Number.isFinite(time)) return '—';
    return new Intl.DateTimeFormat('en-GB', { timeZone: tz, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(time);
  }
  return { normalize, summary, summaryBins, compare, direction, qualify, agreement, estimate, rateFor, settle, consensus, concentration, wilson, benchmark, displayTime, clamp };
});