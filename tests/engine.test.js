const test = require('node:test');
const assert = require('node:assert/strict');
const E = require('../src/engine');
const NOW = Date.UTC(2026, 10, 25, 8, 0, 0);
const item = (venue, side, value, time=NOW-1000) => ({venue,side,value,time,price:100});
test('normalizer rejects stale, future and malformed prints',()=>{
  const raw={venue:'Binance',asset:'BTC',quote:'USDT',side:'BUY',price:'100',quantity:'.2',time:NOW-1000,tradeId:3};
  assert.equal(E.normalize(raw,NOW).value,20);
  assert.equal(E.normalize({...raw,time:NOW-180000},NOW),null);
  assert.equal(E.normalize({...raw,price:0},NOW),null);
  assert.equal(E.normalize({...raw,side:'MAYBE'},NOW),null);
});
test('summary uses strictly bounded non-overlapping ranges',()=>{
  const arr=[item('A','BUY',40,NOW-1000),item('B','SELL',60,NOW-1000),item('A','BUY',50,NOW)];
  const prev=E.summary(arr,NOW-10000,NOW);const next=E.summary(arr,NOW,NOW+10000);
  assert.equal(prev.buyPercent,40);assert.equal(prev.count,2);assert.equal(next.count,1);
});
test('dominance is distinct from momentum: minority buy never bullish dominant',()=>{
  assert.equal(E.direction(42.6),'SELL_DOMINANT');
  assert.equal(E.compare(35,42.6),'BUY_RISING');
});
test('coverage gate blocks sparse or stale venue samples',()=>{
  const x=E.summary([item('A','BUY',10)],NOW-10000,NOW);
  assert.equal(E.qualify(x,0,20000).ok,false);
  assert.ok(E.qualify(x,7000,20000).reasons.includes('STALE_TRADES'));
});
test('forecast settles only on a later disjoint window',()=>{
  const forecast={side:'BUY',baseline:50};
  const future=E.summary(Array.from({length:24},(_,i)=>item(i%2?'A':'B',i<18?'BUY':'SELL',10,NOW+1000)),NOW,NOW+10000);
  assert.equal(E.settle(forecast,future).result,'CORRECT');
  assert.equal(E.settle(forecast,E.summary([],NOW,NOW+10000)).result,'INVALID');
});
test('unverified model cannot show numeric accuracy',()=>{
  const f={side:'BUY',momentum:'fast',dominance:true};
  assert.equal(E.rateFor([{result:'CORRECT',features:f}],f).rate,null);
  assert.equal(E.rateFor(Array.from({length:30},()=>({result:'CORRECT',features:f})),f).rate,100);
});

test('bucket aggregation counts every accepted trade beyond visible tape size',()=>{
  const bins=new Map([[Math.floor((NOW-3000)/1000),{buy:120,sell:30,count:250000,byVenue:new Map([['A',{buy:100,sell:20,count:200000}],['B',{buy:20,sell:10,count:50000}]])}]]);
  const out=E.summaryBins(bins,NOW-10000,NOW);
  assert.equal(out.count,250000);assert.equal(out.buyPercent,80);assert.equal(out.venueCount,2);
});