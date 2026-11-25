/* FlowScope: market feeds, honest signal lab, and terminal UI. */
(() => {
  'use strict';
  const E = window.FlowEngine;
  if (!E) throw new Error('FlowEngine missing');
  const $ = (id) => document.getElementById(id);
  const VENUES = ['Binance', 'Coinbase', 'Kraken', 'OKX', 'Bybit', 'Gate.io', 'Bitstamp', 'Bitget'];
  const QUOTES = { BTC: ['USDT','USD'], ETH: ['USDT','USD'], SOL: ['USDT','USD'], PAXG: ['USDT','USD'] };
  const fmtMoney = (n, precision=2) => n === null || !Number.isFinite(n) ? '—' : '$' + Number(n).toLocaleString('en-US',{minimumFractionDigits:precision,maximumFractionDigits:precision});
  const fmtCompact = n => !Number.isFinite(n) ? '—' : (n >= 1e9 ? '$'+(n/1e9).toFixed(2)+'B' : n>=1e6?'$'+(n/1e6).toFixed(2)+'M':n>=1000?'$'+(n/1000).toFixed(1)+'K':fmtMoney(n));
  const pct = n => n == null ? '—' : n.toFixed(1) + '%';
  const msLabel = n => n == null ? '—' : (n>=1000?(n/1000).toFixed(1)+'s':Math.round(n)+'ms');
  const age = n => n ? Math.max(0,Math.floor((Date.now()-n)/1000))+'s ago' : 'Never';
  const marketKey = (asset, venue) => (venue==='Coinbase'||venue==='Kraken'||venue==='Bitstamp') ? asset+'/USD':asset+'/USDT';
  const displayDate = ts => E.displayTime(ts, Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC');
  const displayFull = ts => displayDate(ts)+'.'+String(ts%1000).padStart(3,'0');
  const resultsKey = 'flowscope-v2-verified-forecasts';
  const selectedKey = 'flowscope-v2-asset';
  let savedAsset = 'BTC'; try { savedAsset=localStorage.getItem(selectedKey)||'BTC' }catch{}
  let asset = ['BTC','ETH','SOL','PAXG'].includes(savedAsset) ? savedAsset : 'BTC';
  let period = 60, epoch = 0, feeds = {}, recent = [], buckets = new Map(), series = [], outcomes = [], pending = null, total = 0;
  let startedAt = Date.now(), lastTrade = null, lastReference = null, lastForecastStart = 0, lastResult = null;
  let strength = { side:'NONE',since:0,candidate:null,changeAt:0 }, momentumSmooth = 50, lastRenderAt = 0;
  let visible = !document.hidden;
  const LIMIT = 30000, STORE_LIMIT = 600, RENDER_LIMIT = 75;
  const venueStatus = n => feeds[n]?.state || 'OFFLINE';
  try { const raw=JSON.parse(localStorage.getItem(resultsKey)||'[]'); if(Array.isArray(raw)) outcomes=raw.filter(x=>x && x.asset && x.timestamp && x.features).slice(-STORE_LIMIT) } catch {}
  function persist() { try { localStorage.setItem(resultsKey,JSON.stringify(outcomes.slice(-STORE_LIMIT))) }catch{} }
  function adapter(name) {
    const A=asset, pair=A+'USDT', coin=A+'-USD';
    const put = (side,p,q,time,id) => accept({venue:name,asset:A,quote:marketKey(A,name).split('/')[1],side,price:p,quantity:q,time,tradeId:id});
    if(name==='Binance') return {url:'wss://data-stream.binance.vision/ws/'+pair.toLowerCase()+'@trade', parse:d=>{if(d.e==='trade')put(d.m?'SELL':'BUY',d.p,d.q,d.T,d.t)}};
    if(name==='Coinbase') return {url:'wss://ws-feed.exchange.coinbase.com', subscribe:{type:'subscribe',product_ids:[coin],channels:['matches']}, parse:d=>{if(d.type==='match')put(d.side==='buy'?'SELL':'BUY',d.price,d.size,Date.parse(d.time),d.trade_id)}};
    if(name==='Kraken') return {url:'wss://ws.kraken.com/v2',subscribe:{method:'subscribe',params:{channel:'trade',symbol:[A+'/USD']}},parse:d=>{if(d.channel==='trade'&&Array.isArray(d.data))for(const t of d.data)put(t.side,t.price,t.qty,Date.parse(t.timestamp),t.trade_id)}};
    if(name==='OKX') return {url:'wss://ws.okx.com:8443/ws/v5/public',subscribe:{op:'subscribe',args:[{channel:'trades',instId:A+'-USDT'}]},parse:d=>{if(d.arg?.channel==='trades'&&Array.isArray(d.data))for(const t of d.data)put(t.side,t.px,t.sz,t.ts,t.tradeId)}};
    if(name==='Bybit') return {url:'wss://stream.bybit.com/v5/public/spot',subscribe:{op:'subscribe',args:['publicTrade.'+pair]},ping:JSON.stringify({op:'ping'}),parse:d=>{if(d.topic?.startsWith('publicTrade.')&&Array.isArray(d.data))for(const t of d.data)put(t.S,t.p,t.v,t.T,t.i)}};
    if(name==='Gate.io') return {url:'wss://api.gateio.ws/ws/v4/', subscribe:{time:Math.floor(Date.now()/1000),channel:'spot.trades',event:'subscribe',payload:[A+'_USDT']}, parse:d=>{if(d.channel==='spot.trades'&&d.event==='update'){const items=Array.isArray(d.result)?d.result:[d.result];for(const t of items)if(t)put(t.side,t.price,t.amount,Number(t.create_time_ms)||Number(t.create_time)*1000,t.id)}}};
    if(name==='Bitstamp') return {url:'wss://ws.bitstamp.net',subscribe:{event:'bts:subscribe',data:{channel:'live_trades_'+A.toLowerCase()+'usd'}},parse:d=>{if(d.event==='trade'&&d.data){const t=d.data;put(Number(t.type)===0?'BUY':Number(t.type)===1?'SELL':'UNKNOWN',t.price,t.amount,t.microtimestamp?Number(t.microtimestamp)/1000:Number(t.timestamp)*1000,t.id)}}};
    if(name==='Bitget') return {url:'wss://ws.bitget.com/v2/ws/public',subscribe:{op:'subscribe',args:[{instType:'SPOT',channel:'trade',instId:pair}]},ping:'ping',parse:d=>{if(d.arg?.channel==='trade'&&Array.isArray(d.data))for(const t of d.data){if(Array.isArray(t))put(t[3],t[1],t[2],t[0],t[4]);else put(t.side,t.price||t.p,t.size||t.sz,t.ts,t.tradeId||t.id)}}};
    return null;
  }
  function accept(raw) {
    const trade=E.normalize(raw),f=feeds[raw.venue];
    if(!trade||!f)return;
    const key=trade.tradeId && raw.venue+':'+asset+':'+trade.tradeId;
    if(key){if(f.ids.has(key))return;f.ids.add(key);if(f.ids.size>14000)f.ids.clear()}
    recent.push(trade);
    const sec = Math.floor(trade.time / 1000);
    let bucket = buckets.get(sec);
    if(!bucket){bucket={buy:0,sell:0,count:0,byVenue:new Map()};buckets.set(sec,bucket)}
    bucket[trade.side==='BUY'?'buy':'sell']+=trade.value;
    bucket.count++;
    const vb=bucket.byVenue.get(trade.venue)||{buy:0,sell:0,count:0};
    vb[trade.side==='BUY'?'buy':'sell']+=trade.value;vb.count++;bucket.byVenue.set(trade.venue,vb);
    total++;f.prints++;f.lastTradeAt=Date.now();f.lastPacketAt=Date.now();f.state='RECEIVING';f.lastQuote=trade;
    lastTrade=trade;
    if(trade.venue==='Binance') {
      lastReference=trade;series.push({t:trade.time,p:trade.price});
      if(series.length>4000)series.splice(0,series.length-4000);
    }
    if(recent.length>LIMIT)recent.splice(0,recent.length-LIMIT);
  }
  function closeFeed(f) {
    if(f.timer)clearTimeout(f.timer);f.timer=null;
    if(f.ws){const s=f.ws;f.ws=null;s.onclose=s.onopen=s.onerror=s.onmessage=null;try{s.close()}catch{}}
  }
  function reconnect(name,why){const f=feeds[name];if(!f||f.unsupported)return;closeFeed(f);f.state='RETRYING';f.error=why;f.attempt++;f.timer=setTimeout(()=>{f.timer=null;openFeed(name)},Math.min(35000,1250*2**Math.min(f.attempt,5))+Math.random()*500)}
  function openFeed(name){const f=feeds[name];if(!f||f.unsupported)return;closeFeed(f);f.state='CONNECTING';f.openAt=Date.now();f.lastPacketAt=Date.now();const generation=epoch;let socket;try{socket=new WebSocket(f.adapter.url)}catch{return reconnect(name,'WebSocket blocked')}
    f.ws=socket;
    socket.onopen=()=>{if(generation!==epoch||f.ws!==socket)return;f.state='CONNECTED';f.attempt=0;f.lastPacketAt=Date.now();try{if(f.adapter.subscribe)socket.send(JSON.stringify(f.adapter.subscribe))}catch{reconnect(name,'Subscription failed')}};
    socket.onmessage=e=>{if(generation!==epoch||f.ws!==socket)return;f.lastPacketAt=Date.now();try{
      const d=JSON.parse(e.data);
      if(d.event==='error'||d.success===false||(d.code&&d.msg&&String(d.code)!=='0')){reconnect(name,'Exchange rejected subscription');return}
      f.adapter.parse(d);
    }catch(err){f.error='Unrecognized message'}};
    socket.onerror=()=>{if(f.ws===socket)reconnect(name,'Socket error')};
    socket.onclose=()=>{if(f.ws===socket)reconnect(name,'Disconnected')};
  }
  function boot(){epoch++;Object.values(feeds).forEach(closeFeed);feeds={};recent=[];buckets.clear();series=[];total=0;lastTrade=lastReference=null;pending=null;lastForecastStart=0;lastResult=null;startedAt=Date.now();strength={side:'NONE',since:0,candidate:null,changeAt:0};momentumSmooth=50;
    for(const name of VENUES){const unsupported=asset==='PAXG'&&['Bybit','Bitstamp','Bitget'].includes(name);feeds[name]={adapter:unsupported?null:adapter(name),unsupported,state:unsupported?'NOT ENABLED':'CONNECTING',ws:null,timer:null,attempt:0,error:'',openAt:0,lastPacketAt:0,lastTradeAt:0,prints:0,ids:new Set(),lastQuote:null};}
    for(const name of VENUES)if(!feeds[name].unsupported)openFeed(name);
    document.querySelectorAll('[data-symbol]').forEach(b=>b.classList.toggle('active',b.dataset.symbol===asset));
    $('noticeGold').hidden=asset!=='PAXG';$('symbolCode').textContent=asset+'/USDT · Binance spot reference';render();
  }
  function watchdog(){if(document.hidden)return;const now=Date.now();for(const n of VENUES){const f=feeds[n];if(f.unsupported)continue;
    if(f.ws?.readyState===WebSocket.CONNECTING&&now-f.openAt>16000)reconnect(n,'Handshake timeout');
    else if(f.ws?.readyState===WebSocket.OPEN&&now-f.lastPacketAt>80000)reconnect(n,'No packets');
    else if(!f.ws&&!f.timer)reconnect(n,'Socket absent');
    else if(f.ws?.readyState===WebSocket.OPEN&&f.adapter.ping&&now-f.lastPacketAt>18000){try{f.ws.send(f.adapter.ping)}catch{reconnect(n,'Ping failed')}}
  }}
  function setStreak(candidate,now,valid){
    if(!valid||candidate==='NONE') {strength={side:'NONE',since:0,candidate:null,changeAt:0};return}
    if(strength.side===candidate){strength.candidate=null;return}
    if(strength.candidate?.side!==candidate){strength.candidate={side:candidate,at:now};return}
    if(now-strength.candidate.at>=3000){strength={side:candidate,since:strength.candidate.at,candidate:null,changeAt:now}}
  }
  function streakUI(side){const on=strength.side===side,ts=on?Math.floor((Date.now()-strength.since)/1000):0,id=side==='BUY'?'buy':'sell',level=ts>=180?4:ts>=60?3:ts>=25?2:ts>0?1:0;
    $(id+'Streak').textContent=on?String(Math.floor(ts/60)).padStart(2,'0')+':'+String(ts%60).padStart(2,'0'):'00:00';
    $(id+'Strength').textContent=['IDLE','BUILDING','DEVELOPING','SUSTAINED','EXTENDED'][level];
    $(id+'Meter').style.width=Math.min(100,ts/180*100)+'%';
    $(id+'Tile').dataset.level=level;
    $(id+'StreakDetail').textContent=on?'Dominant direction held across confirmed updates':'No qualifying dominance streak';
  }
  const lagMs=()=>lastTrade?Date.now()-lastTrade.receivedAt:Infinity;
  function calculate(now){const end=Math.floor(now/1000)*1000;const last30=E.summaryBins(buckets,end-30000,end),prev10=E.summaryBins(buckets,end-20000,end-10000),last10=E.summaryBins(buckets,end-10000,end),last60=E.summaryBins(buckets,end-60000,end),window=E.summaryBins(buckets,end-period*1000,end);
    const liveCount=VENUES.filter(n=>feeds[n]?.lastTradeAt&&now-feeds[n].lastTradeAt<15000).length;
    const q=E.qualify(window,lagMs(),now-startedAt,{minTrades:40,minVenues:2,minAge:10000});
    if(liveCount<2)q.reasons.push('VENUE_FEEDS_STALE');q.ok=q.reasons.length===0;
    return {last30,prev10,last10,last60,window,liveCount,q};
  }
  function analyzeForecast(now,c){
    if(document.hidden){pending=null;return}
    // A forecast is issued at most once per ten seconds. The target observation is *only* the subsequent ten seconds.
    if(pending&&now>=pending.until+500){
      const forward=E.summaryBins(buckets,pending.at,pending.until),result=E.settle(pending,forward,{minForward:20,minVenues:2});
      if(now-pending.until>4500)result.result='INVALID';
      const row={timestamp:pending.issuedAt,asset,side:pending.side,baseline:pending.baseline,features:pending.features,result:result.result,delta:result.delta,reason:result.reason};
      outcomes.push(row);outcomes=outcomes.slice(-STORE_LIMIT);lastResult=row;persist();pending=null;
    }
    if(pending||!c.q.ok||Date.now()-startedAt<30000)return;
    if(now-lastForecastStart<10000)return;
    lastForecastStart=now;
    const end=Math.floor(now/1000)*1000;
    const short=E.summaryBins(buckets,end-10000,end),earlier=E.summaryBins(buckets,end-20000,end-10000),main=E.summaryBins(buckets,end-60000,end);
    const strict=E.qualify(short,lagMs(),now-startedAt,{minTrades:30,minVenues:2,minAge:30000});
    const decision=E.estimate(short,earlier,main,strict);
    if(decision.side==='ABSTAIN')return;
    const futureStart=end+1000; // Skip incomplete current second: no pre-issuance prints leak into target.
    pending={issuedAt:now,at:futureStart,until:futureStart+10000,asset,side:decision.side,baseline:short.buyPercent,features:decision.features,reason:decision.reason};
  }
  function chart(){const canvas=$('priceCanvas');if(!canvas)return;const context=canvas.getContext('2d');if(!context)return;const r=canvas.getBoundingClientRect();const dpr=Math.min(2,window.devicePixelRatio||1),w=Math.max(240,r.width),h=Math.max(130,r.height);canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr);context.setTransform(dpr,0,0,dpr,0,0);context.clearRect(0,0,w,h);
    let points=series.filter(x=>x.t>=Date.now()-180000).slice(-700);
    if(points.length<2){context.fillStyle='#94adb7';context.font='12px sans-serif';context.fillText('Waiting for Binance spot executions…',18,h/2);return}
    let lo=Math.min(...points.map(x=>x.p)),hi=Math.max(...points.map(x=>x.p));const pad=(hi-lo)*.12||hi*.0002;lo-=pad;hi+=pad;const start=points[0].t,end=points[points.length-1].t;
    context.strokeStyle='#274451';context.lineWidth=1;for(let i=0;i<5;i++){let y=16+(h-34)*i/4;context.beginPath();context.moveTo(0,y);context.lineTo(w,y);context.stroke()}
    context.beginPath();points.forEach((p,i)=>{let x=12+(w-24)*(p.t-start)/Math.max(end-start,1),y=14+(h-34)*(1-(p.p-lo)/(hi-lo));if(i===0)context.moveTo(x,y);else context.lineTo(x,y)});context.strokeStyle=points[points.length-1].p>=points[0].p?'#39dcaa':'#fb718d';context.lineWidth=2;context.stroke();
    context.fillStyle='#b4c9d0';context.font='11px sans-serif';context.fillText('BINANCE '+asset+'/USDT · LAST '+points.length+' PRINTS',12,13);
    context.fillText(formatNumber(hi),12,h-4);context.textAlign='right';context.fillText(formatNumber(lo),w-12,h-4);context.textAlign='left';
  }
  function formatNumber(n){return n.toLocaleString('en-US',{maximumFractionDigits:n<10?5:2})}
  function row(id,value){$(id).textContent=value}
  function render(){const now=Date.now();lastRenderAt=now;
    for (const second of buckets.keys()) if(second < Math.floor((now-380000)/1000)) buckets.delete(second);const c=calculate(now);analyzeForecast(now,c);
    const {window:w,last10,last30,last60,q,liveCount}=c,connected=VENUES.filter(n=>feeds[n]?.ws?.readyState===WebSocket.OPEN).length;
    $('statusLed').classList.toggle('active',liveCount>0);row('connectionState',liveCount?`${liveCount} RECEIVING · ${connected} CONNECTED`:connected?`${connected} CONNECTED · WAITING`:'CONNECTING / BLOCKED');
    const freshRef=lastReference&&now-lastReference.receivedAt<10000;
    row('referencePrice',freshRef?formatNumber(lastReference.price)+' USDT':'—');
    row('referenceAge',freshRef?`Binance BTC/USDT`.replace('BTC',asset)+` · trade ${displayFull(lastReference.time)} · ${age(lastReference.receivedAt)}`:'Reference feed unavailable or stale');
    row('referenceLatency',freshRef?msLabel(Math.max(0,lastReference.receivedAt-lastReference.time)):'—');
    row('lastVenue',lastTrade?`${lastTrade.venue} ${lastTrade.asset}/${lastTrade.quote} · ${displayFull(lastTrade.time)}`:'No verified trades received');
    row('buyNotional',fmtCompact(w.buy));row('sellNotional',fmtCompact(w.sell));row('buyPercent',pct(w.buyPercent));row('sellPercent',w.buyPercent==null?'—':pct(100-w.buyPercent));row('netDelta',w.count?(w.buy>=w.sell?'+':'−')+fmtCompact(Math.abs(w.buy-w.sell)):'—');
    $('netDelta').className=w.buy>=w.sell?'positive':'negative';
    $('pressureBar').classList.toggle('empty',w.buyPercent==null);$('pressureFill').style.width=(w.buyPercent??0)+'%';
    row('sampleInfo',`${w.count.toLocaleString()} executions / ${w.venueCount} venues · ${period}s`);
    row('referenceLabel',asset+'/USDT · Binance reference (not a consolidated price)');
    const dominant=E.direction(w.buyPercent,55,2);const strict=q.ok&&(dominant==='BUY_DOMINANT'||dominant==='SELL_DOMINANT');
    const nextCandidate=strict?(dominant==='BUY_DOMINANT'?'BUY':'SELL'):'NONE';setStreak(nextCandidate,now,q.ok);streakUI('BUY');streakUI('SELL');
    row('dominance',!q.ok?'UNCONFIRMED':dominant.replace('_',' '));$('dominance').className=dominant==='BUY_DOMINANT'?'positive':dominant==='SELL_DOMINANT'?'negative':'neutral';
    row('momentum',last10.buyPercent==null||c.prev10.buyPercent==null?'—':(last10.buyPercent-c.prev10.buyPercent>=0?'+':'')+(last10.buyPercent-c.prev10.buyPercent).toFixed(1)+'pp');
    row('trendExplanation',!q.ok?'Need more fresh trades from at least two venues.':dominant==='BUY_DOMINANT'?'Buy-side notional dominates the selected window.':dominant==='SELL_DOMINANT'?'Sell-side notional dominates the selected window.':'Neither side dominates beyond the noise band.');
    row('quality',q.ok?'LIVE SAMPLE · not a price signal':`QUALITY HOLD · ${q.reasons.join(' · ')||'waiting'}`);
    row('trend30',pct(last30.buyPercent));row('trend60',pct(last60.buyPercent));
    const d= pending ? {side:pending.side,reason:pending.reason,features:pending.features} : {side:'ABSTAIN',reason:q.ok?'No qualifying, non-overlapping 10-second forecast right now.':'Not enough live coverage.'};
    row('forecastDirection',pending?(d.side==='BUY'?'BUY PRESSURE ↑':'SELL PRESSURE ↑'):'WAIT / NO EDGE');
    $('forecastDirection').className=pending?(d.side==='BUY'?'positive':'negative'):'neutral';
    row('forecastReason',d.reason);row('forecastCountdown',pending?Math.max(0,Math.ceil((pending.until-now)/1000))+'s to outcome':'—');
    const assetOutcomes=outcomes.filter(x=>x.asset===asset),valid=assetOutcomes.filter(x=>x.result==='CORRECT'||x.result==='WRONG'),wins=valid.filter(x=>x.result==='CORRECT').length;
    const matched= pending?E.rateFor(assetOutcomes,pending.features):{count:0,rate:null};
    row('empiricalRate',pending?(matched.rate==null?`UNVERIFIED · ${matched.count}/30 comparable`:matched.rate.toFixed(1)+'% observed continuation · '+matched.count+' checks'):'Awaiting validated forecast');
    row('forecastTrack',pending?'OPEN · 10s forward window':'—');
    row('forecastStatus',valid.length>=30?`${(100*wins/valid.length).toFixed(1)}% directional hits · ${valid.length} checks`:`${valid.length}/30 directional outcomes · unvalidated`);
    row('lastOutcome',lastResult?`${lastResult.side}: ${lastResult.result}`:'No completed forecast yet');
    row('outcomeDetails',lastResult?`${lastResult.delta==null?'—':(lastResult.delta>=0?'+':'')+lastResult.delta.toFixed(2)+'pp'} · ${displayDate(lastResult.timestamp)}`:'Forecasts require a complete, observed next 10s');
    row('observations',outcomes.filter(x=>x.asset===asset).length.toLocaleString()+' recorded outcomes');
    row('totalReceived',total.toLocaleString()+' received');row('bufferSize',recent.length.toLocaleString()+' retained; latest '+RENDER_LIMIT+' displayed');
    row('timezone',Intl.DateTimeFormat().resolvedOptions().timeZone||'UTC');
    $('venueRows').innerHTML=VENUES.map(n=>{const f=feeds[n],fresh=f.lastTradeAt&&now-f.lastTradeAt<15000;const label=f.unsupported?'NOT ENABLED':fresh?'RECEIVING':f.state==='RECEIVING'?'QUIET / STALE':f.state;return `<div class="venue-row"><span><i class="led-small ${fresh?'good':f.unsupported?'bad':''}"></i><b>${n}</b><small>${asset}/${marketKey(asset,n).split('/')[1]}</small></span><span class="venue-right">${label}<small>${f.prints.toLocaleString()} prints · ${age(f.lastTradeAt)}</small></span></div>`}).join('');
    const filterVenue=$('filterVenue').value,filterSide=$('filterSide').value,min=Number($('minTrade').value)||0;
    const display=recent.filter(t=>(filterVenue==='ALL'||filterVenue===t.venue)&&(filterSide==='ALL'||filterSide===t.side)&&t.value>=min).slice(-RENDER_LIMIT).reverse();
    $('tapeRows').innerHTML=display.length?display.map(t=>`<tr><td>${displayFull(t.time)}</td><td>${t.venue}<small>${t.quote}</small></td><td class="${t.side==='BUY'?'positive':'negative'}">${t.side}</td><td>${formatNumber(t.price)} <small>${t.quote}</small></td><td>${fmtCompact(t.value)}</td></tr>`).join(''):'<tr><td class="empty" colspan="5">No matching public executions received yet.</td></tr>';
    $('historyRows').innerHTML=assetOutcomes.length?assetOutcomes.slice(-12).reverse().map(x=>`<tr><td>${displayDate(x.timestamp)}</td><td>${x.side}</td><td>${x.result}</td><td>${x.delta==null?'—':(x.delta>=0?'+':'')+x.delta.toFixed(2)+'pp'}</td></tr>`).join(''):'<tr><td colspan="4" class="empty">Waiting for verified non-overlapping observations.</td></tr>';
    chart();
  }
  document.querySelectorAll('[data-symbol]').forEach(b=>b.addEventListener('click',()=>{asset=b.dataset.symbol;try{localStorage.setItem(selectedKey,asset)}catch{}boot()}));
  $('periodSelect').addEventListener('change',e=>{period=Number(e.target.value);strength={side:'NONE',since:0,candidate:null,changeAt:0};render()});
  $('reconnect').addEventListener('click',()=>{for(const n of VENUES)if(!feeds[n].unsupported){feeds[n].attempt=0;openFeed(n)}render()});
  ['filterVenue','filterSide','minTrade'].forEach(id=>$(id).addEventListener('input',render));
  $('clearOutcomes').addEventListener('click',()=>{outcomes=outcomes.filter(x=>x.asset!==asset);persist();render()});
  document.addEventListener('visibilitychange',()=>{visible=!document.hidden;if(!visible){pending=null;strength={side:'NONE',since:0,candidate:null,changeAt:0}}else{watchdog();render()}});
  window.addEventListener('online',()=>{watchdog();render()});
  window.addEventListener('resize',()=>chart());
  boot();setInterval(()=>{if(!document.hidden)render()},1000);setInterval(watchdog,7500);
})();