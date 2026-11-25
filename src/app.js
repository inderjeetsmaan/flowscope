/* FlowScope terminal: real executed trades, market quality, and forward-only research. */
(() => {
'use strict';
const E=window.FlowEngine;
if(!E)throw Error('FlowEngine unavailable');
const $=id=>document.getElementById(id);
const VENUES=['Binance','Coinbase','Kraken','OKX','Bybit','Gate.io','Bitstamp','Bitget'];
const FRAMES=[15,30,60,180,300,900,1800,3600],frameName=s=>s===3600?'1h':s>=60?s/60+'m':s+'s';
const marketQuote=n=>['Coinbase','Kraken','Bitstamp'].includes(n)?'USD':'USDT';
const money=n=>n==null||!Number.isFinite(n)?'—':n>=1e6?'$'+(n/1e6).toFixed(2)+'M':n>=1000?'$'+(n/1e3).toFixed(1)+'K':'$'+n.toFixed(2);
const percent=n=>n==null||!Number.isFinite(n)?'—':n.toFixed(1)+'%';
const fmtPrice=n=>n==null||!Number.isFinite(n)?'—':n.toLocaleString('en-US',{maximumFractionDigits:n<10?5:2});
const fmtLocal=t=>{const n=Math.round(t);return new Date(n).toLocaleTimeString('en-GB',{hour12:false})+'.'+String(((n%1000)+1000)%1000).padStart(3,'0')};
const set=(id,value)=>{const el=$(id);if(el)el.textContent=value};
const key='flowscope-research-v3',selectedKey='flowscope-asset-v3';
let asset='BTC';try{asset=localStorage.getItem(selectedKey)||'BTC'}catch{}if(!['BTC','ETH','SOL','PAXG'].includes(asset))asset='BTC';
let quote='USDT',feeds={},epoch=0,raw=[],series={USDT:[],USD:[]},buckets={USD:new Map(),USDT:new Map()},total=0,openedAt=Date.now(),lastPrint=null,references={USD:null,USDT:null},results=[],pending=null,lastIssued=0,lastResult=null;
let selected=new Set([15,30,60]),frameStreaks=Object.fromEntries(FRAMES.map(s=>[s,{side:'NONE',candidate:0,since:0}]));
let quoteEpoch=Date.now(),lastTape=0,lastChart=0,displayVersion=0,forecastVersion=1,lastResize=0;
try{const x=JSON.parse(localStorage.getItem(key)||'[]');if(Array.isArray(x))results=x.filter(v=>v&&v.version===forecastVersion).slice(-1200)}catch{}
function save(){try{localStorage.setItem(key,JSON.stringify(results.slice(-1200)))}catch{}}
function correctTime(t){const v=Number(t);if(!Number.isFinite(v))return NaN;return Math.round(v)}
function parseVenue(name,d){
 const a=asset,pair=a+'USDT',out=[];
 const push=(side,p,q,time,id)=>out.push({venue:name,asset:a,quote:marketQuote(name),side:String(side).toUpperCase(),price:p,quantity:q,time:correctTime(time),tradeId:id});
 if(name==='Binance'&&d.e==='trade')push(d.m?'SELL':'BUY',d.p,d.q,d.T,d.t);
 if(name==='Coinbase'&&d.type==='match')push(d.side==='buy'?'SELL':'BUY',d.price,d.size,Date.parse(d.time),d.trade_id);
 if(name==='Kraken'&&d.channel==='trade'&&Array.isArray(d.data))for(const x of d.data)push(x.side,x.price,x.qty,Date.parse(x.timestamp),x.trade_id);
 if(name==='OKX'&&d.arg?.channel==='trades'&&Array.isArray(d.data))for(const x of d.data)push(x.side,x.px,x.sz,x.ts,x.tradeId);
 if(name==='Bybit'&&d.topic?.startsWith('publicTrade.')&&Array.isArray(d.data))for(const x of d.data)push(x.S,x.p,x.v,x.T,x.i);
 if(name==='Gate.io'&&d.channel==='spot.trades'&&d.event==='update'){const rows=Array.isArray(d.result)?d.result:[d.result];for(const x of rows)if(x)push(x.side,x.price,x.amount,x.create_time_ms||Number(x.create_time)*1000,x.id)}
 if(name==='Bitstamp'&&d.event==='trade'&&d.data){const x=d.data;push(Number(x.type)===0?'BUY':Number(x.type)===1?'SELL':'INVALID',x.price,x.amount,x.microtimestamp?Number(x.microtimestamp)/1000:Number(x.timestamp)*1000,x.id)}
 if(name==='Bitget'&&d.arg?.channel==='trade'&&Array.isArray(d.data))for(const x of d.data){if(Array.isArray(x))push(x[3],x[1],x[2],x[0],x[4]);else push(x.side,x.price??x.p,x.size??x.sz,x.ts,x.tradeId??x.id)}
 return out;
}
function config(name){
 const a=asset,b=a+'USDT';
 const shared={Binance:{url:'wss://data-stream.binance.vision/ws/'+b.toLowerCase()+'@trade'},
 Coinbase:{url:'wss://ws-feed.exchange.coinbase.com',sub:{type:'subscribe',product_ids:[a+'-USD'],channels:['matches']}},
 Kraken:{url:'wss://ws.kraken.com/v2',sub:{method:'subscribe',params:{channel:'trade',symbol:[a+'/USD']}}},
 OKX:{url:'wss://ws.okx.com:8443/ws/v5/public',sub:{op:'subscribe',args:[{channel:'trades',instId:a+'-USDT'}]}},
 Bybit:{url:'wss://stream.bybit.com/v5/public/spot',sub:{op:'subscribe',args:['publicTrade.'+b]},ping:'{"op":"ping"}'},
 'Gate.io':{url:'wss://api.gateio.ws/ws/v4/',sub:{time:Math.floor(Date.now()/1000),channel:'spot.trades',event:'subscribe',payload:[a+'_USDT']}},
 Bitstamp:{url:'wss://ws.bitstamp.net',sub:{event:'bts:subscribe',data:{channel:'live_trades_'+a.toLowerCase()+'usd'}}},
 Bitget:{url:'wss://ws.bitget.com/v2/ws/public',sub:{op:'subscribe',args:[{instType:'SPOT',channel:'trade',instId:b}]},ping:'ping'}};
 return shared[name];
}
function put(rawTrade){
 const t=E.normalize(rawTrade),f=feeds[rawTrade.venue];if(!t||!f)return;
 const id=t.tradeId?rawTrade.venue+'/'+asset+'/'+t.tradeId:null;
 if(id){if(f.ids.has(id))return;f.ids.add(id);if(f.ids.size>16000)f.ids.clear()}
 raw.push(t);if(raw.length>30000)raw.splice(0,raw.length-30000);
 const sec=Math.floor(t.time/1000),store=buckets[t.quote];let b=store.get(sec);
 if(!b){b={buy:0,sell:0,count:0,byVenue:new Map()};store.set(sec,b)}
 b[t.side==='BUY'?'buy':'sell']+=t.value;b.count++;
 const v=b.byVenue.get(t.venue)||{buy:0,sell:0,count:0};v[t.side==='BUY'?'buy':'sell']+=t.value;v.count++;b.byVenue.set(t.venue,v);
 total++;f.count++;f.lastTrade=Date.now();f.lastPacket=Date.now();f.status='RECEIVING';
 if(!lastPrint||t.time>=lastPrint.time)lastPrint=t;
 const refVenue=t.quote==='USDT'?'Binance':'Coinbase';
 if(t.venue===refVenue){if(!references[t.quote]||t.time>=references[t.quote].time)references[t.quote]=t;
 const list=series[t.quote];list.push({t:t.time,p:t.price,received:t.receivedAt});if(list.length>6000)list.splice(0,1000)}
}
function close(f){if(!f)return;clearTimeout(f.timer);f.timer=null;if(f.ws){const s=f.ws;f.ws=null;s.onopen=s.onmessage=s.onclose=s.onerror=null;try{s.close()}catch{}}}
function retry(n,reason){const f=feeds[n];if(!f||f.disabled)return;close(f);f.status='RETRYING';f.error=reason;f.retries++;const delay=Math.min(45000,1000*2**Math.min(f.retries,5))+Math.random()*500;f.timer=setTimeout(()=>{f.timer=null;connect(n)},delay)}
function connect(n){const f=feeds[n];if(!f||f.disabled)return;close(f);f.status='CONNECTING';f.since=Date.now();f.lastPacket=f.since;const gen=epoch;try{const s=new WebSocket(f.config.url);f.ws=s;s.onopen=()=>{if(gen!==epoch||f.ws!==s)return;f.status='CONNECTED';f.retries=0;f.lastPacket=Date.now();try{if(f.config.sub)s.send(JSON.stringify(f.config.sub))}catch{retry(n,'SUBSCRIBE_FAILED')}};s.onmessage=e=>{if(gen!==epoch||f.ws!==s)return;f.lastPacket=Date.now();try{const d=JSON.parse(e.data);if(d.event==='error'||d.success===false||(d.code&&String(d.code)!=='0'&&d.msg)){retry(n,'SUBSCRIPTION_REJECTED');return}for(const t of parseVenue(n,d))put(t)}catch(err){f.error='PARSER_ERROR'}};s.onerror=()=>{if(f.ws===s)retry(n,'SOCKET_ERROR')};s.onclose=()=>{if(f.ws===s)retry(n,'CLOSED')}}catch{retry(n,'BLOCKED')}}
function watchdog(){if(document.hidden)return;const now=Date.now();for(const n of VENUES){const f=feeds[n];if(f.disabled)continue;if(f.ws?.readyState===WebSocket.CONNECTING&&now-f.since>18000)retry(n,'CONNECT_TIMEOUT');else if(f.ws?.readyState===WebSocket.OPEN&&now-f.lastPacket>85000)retry(n,'IDLE_TIMEOUT');else if(!f.ws&&!f.timer)retry(n,'DISCONNECTED');else if(f.ws?.readyState===WebSocket.OPEN&&f.config.ping&&now-f.lastPacket>18000){try{f.ws.send(f.config.ping)}catch{retry(n,'PING_ERROR')}}}}
function resetMarket(){epoch++;Object.values(feeds).forEach(close);buckets={USD:new Map(),USDT:new Map()};raw=[];series={USD:[],USDT:[]};references={USD:null,USDT:null};total=0;openedAt=Date.now();quoteEpoch=openedAt;lastPrint=null;pending=null;lastIssued=0;frameStreaks=Object.fromEntries(FRAMES.map(s=>[s,{side:'NONE',candidate:0,since:0}]));feeds={};for(const n of VENUES){const disabled=asset==='PAXG'&&['Bybit','Bitstamp','Bitget'].includes(n);feeds[n]={config:config(n),disabled,status:disabled?'UNSUPPORTED':'CONNECTING',ws:null,timer:null,retries:0,count:0,lastTrade:0,lastPacket:0,since:0,error:'',ids:new Set()};if(!disabled)connect(n)}document.querySelectorAll('[data-symbol]').forEach(b=>b.classList.toggle('active',b.dataset.symbol===asset));$('noticeGold').hidden=asset!=='PAXG';render()}
function summarize(sec,now=Date.now()){const end=Math.floor(now/1000)*1000;return E.summaryBins(buckets[quote],end-sec*1000,end)}
function freshVenues(now=Date.now()){return VENUES.filter(n=>marketQuote(n)===quote&&feeds[n]?.lastTrade&&now-feeds[n].lastTrade<15000)}
function windowData(sec,now){const w=summarize(sec,now),complete=now-quoteEpoch>=sec*1000+1000,alive=freshVenues(now);const check=E.qualify(w,alive.length?Math.min(...alive.map(n=>now-feeds[n].lastTrade)):Infinity,now-quoteEpoch,{minTrades:30,minVenues:2,minAge:10000});const concentration=E.concentration(w);const ready=complete&&alive.length>=2&&check.ok&&concentration!==null&&concentration<=.92;return {w,complete,ready,concentration,progress:Math.min(100,Math.floor(100*(now-quoteEpoch)/(sec*1000))),status:!complete?'WARMUP':alive.length<2?'INSUFFICIENT FEEDS':concentration>.92?'VENUE CONCENTRATION':ready?'QUALIFIED':'LOW SAMPLE'} }
function referenceAt(t){const a=series[quote].filter(x=>x.t<=t&&x.t>=t-3500).sort((a,b)=>b.t-a.t);return a[0]||null}
function labels(issued,until){const older=series[quote].filter(x=>x.t<=issued&&x.t>=issued-4500).sort((a,b)=>b.t-a.t)[0],later=series[quote].filter(x=>x.t>=until-1500&&x.t<=until+1500).sort((a,b)=>Math.abs(a.t-until)-Math.abs(b.t-until))[0];if(!older||!later)return {returnPct:null,side:'INVALID'};const change=100*(later.p/older.p-1);return {returnPct:change,side:change>.015?'BUY':change<-.015?'SELL':'FLAT'} }
function research(now){
 // One forecast covers one 10s future interval; never score missing or late data as success.
 if(document.hidden){pending=null;return}
 const end=Math.floor(now/1000)*1000;
 if(pending&&now>=pending.until+1200){const f=pending;pending=null;const future=E.summaryBins(buckets[f.quote],f.start,f.until);
   const quality=E.qualify(future,0,30000,{minTrades:20,minVenues:2,minAge:10000});
   const actual=future.buyPercent==null?'INVALID':future.buyPercent-f.baseline>2?'BUY':future.buyPercent-f.baseline< -2?'SELL':'FLAT';
   const flowResult=!quality.ok||now-f.until>5000?'INVALID':actual==='FLAT'?'FLAT':actual===f.side?'CORRECT':'WRONG';
   const price=labels(f.issuedAt,f.until),priceResult=price.side==='INVALID'?'INVALID':price.side==='FLAT'?'FLAT':price.side===f.side?'CORRECT':'WRONG';
   const record={version:forecastVersion,asset:f.asset,quote:f.quote,issuedAt:f.issuedAt,side:f.side,baseline:f.baseline,flowActual:actual,flowDelta:future.buyPercent==null?null:future.buyPercent-f.baseline,flowResult,priceResult,returnPct:price.returnPct,features:f.features};
   results.push(record);if(results.length>1200)results=results.slice(-1200);lastResult=record;save();
 }
 if(pending||now-lastIssued<11000||now-quoteEpoch<30000)return;
 // Predict pressure of the subsequent *nonoverlapping* 10s window, not price.
 const last=E.summaryBins(buckets[quote],end-10000,end),previous=E.summaryBins(buckets[quote],end-20000,end-10000),main=E.summaryBins(buckets[quote],end-60000,end),qual=windowData(30,now);
 const signal=E.estimate(last,previous,main,E.qualify(last,freshVenues(now).length>=2?0:Infinity,now-quoteEpoch,{minTrades:30,minVenues:2,minAge:30000}));
 if(!qual.ready||signal.side==='ABSTAIN')return;
 lastIssued=now;pending={issuedAt:now,start:end+1000,until:end+11000,asset,quote,side:signal.side,baseline:last.buyPercent,features:signal.features,reason:signal.reason};
}
function setStreak(seconds,state,target,now,valid){
 if(!valid){state.side='NONE';state.start=0;state.candidate=0;return}
 if(target===state.side){state.candidate=0;return}
 if(!state.candidate||state.candidate.side!==target)state.candidate={side:target,at:now};
 else if(now-state.candidate.at>=3000){state.side=target;state.start=target==='NONE'?0:state.candidate.at;state.candidate=0}
}
function fmtDuration(n){return String(Math.floor(n/60)).padStart(2,'0')+':'+String(n%60).padStart(2,'0')}
function drawChart(now){if(now-lastChart<2200)return;lastChart=now;const canvas=$('priceCanvas'),ctx=canvas.getContext('2d');if(!ctx)return;const box=canvas.getBoundingClientRect(),w=Math.max(200,box.width),h=Math.max(140,box.height),dpr=Math.min(2,window.devicePixelRatio||1);canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr);ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);
 const values=series[quote].filter(x=>x.t>=now-180000).sort((a,b)=>a.t-b.t);const points=values.filter((x,i)=>i%Math.max(1,Math.ceil(values.length/700))===0);
 if(points.length<2){ctx.fillStyle='#9aafbb';ctx.font='12px sans-serif';ctx.fillText('Awaiting '+quote+' reference prints',16,h/2);return}
 let lo=Math.min(...points.map(x=>x.p)),hi=Math.max(...points.map(x=>x.p));const pad=(hi-lo)*.1||hi*.0001;lo-=pad;hi+=pad;const start=points[0].t,end=points[points.length-1].t;
 ctx.strokeStyle='#2b4657';ctx.lineWidth=1;for(let i=1;i<5;i++){let y=12+(h-25)*i/5;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(w,y);ctx.stroke()}
 ctx.beginPath();points.forEach((p,i)=>{const x=12+(w-24)*(p.t-start)/Math.max(1,end-start),y=14+(h-36)*(1-(p.p-lo)/(hi-lo));if(i)ctx.lineTo(x,y);else ctx.moveTo(x,y)});ctx.strokeStyle=points.at(-1).p>=points[0].p?'#3cdda8':'#f9748e';ctx.lineWidth=2;ctx.stroke();ctx.fillStyle='#acc3cc';ctx.font='11px sans-serif';ctx.fillText((quote==='USDT'?'BINANCE':'COINBASE')+' '+asset+'/'+quote+' · 3 MIN',12,13);
}
function renderFrames(now){
 const frames=FRAMES.map(sec=>{const d=windowData(sec,now),direction=d.ready?E.direction(d.w.buyPercent,55,2):'UNKNOWN',state=frameStreaks[sec],target=direction==='BUY_DOMINANT'?'BUY':direction==='SELL_DOMINANT'?'SELL':'NONE';setStreak(sec,state,target,now,d.ready);
 const recent=summarize(10,now),older=E.summaryBins(buckets[quote],Math.floor(now/1000)*1000-20000,Math.floor(now/1000)*1000-10000);
 const outlook=d.ready?E.estimate(recent,older,d.w,E.qualify(recent,0,now-quoteEpoch,{minTrades:30,minVenues:2,minAge:30000})):{side:'ABSTAIN'};
 const momentum=recent.buyPercent==null||older.buyPercent==null?null:recent.buyPercent-older.buyPercent;
 return {sec,...d,direction,state,outlook,momentum}});
 const map=new Map(frames.map(x=>[x.sec,x])),p=map.get(+$('pressureFrame').value),s=map.get(+$('streakFrame').value),o=map.get(+$('outlookFrame').value);
 set('buyPercent',p.ready?percent(p.w.buyPercent):'—');set('sellPercent',p.ready?percent(100-p.w.buyPercent):'—');
 $('pressureFill').style.width=(p.ready?p.w.buyPercent:0)+'%';$('pressureBar').classList.toggle('empty',!p.ready);
 set('dominance',p.ready?p.direction.replace('_',' '):p.status);
 $('dominance').className=p.ready&&p.direction==='BUY_DOMINANT'?'positive':p.ready&&p.direction==='SELL_DOMINANT'?'negative':'neutral';
 set('momentum',p.momentum==null?'—':(p.momentum>=0?'+':'')+p.momentum.toFixed(1)+'pp');
 set('trendExplanation',p.ready?'Full '+frameName(p.sec)+' window · '+p.w.count+' trades · '+p.w.venueCount+' '+quote+' venues':'No complete/healthy '+frameName(p.sec)+' window. Partial values withheld.');
 set('quality',p.status+(p.ready?' · '+p.w.count.toLocaleString()+' prints':' · '+p.progress+'% elapsed'));
 for(const side of ['BUY','SELL']){const id=side==='BUY'?'buy':'sell',t=s.ready&&s.state.side===side?Math.floor((now-s.state.start)/1000):0;set(id+'Streak',fmtDuration(t));set(id+'Strength',t?'DOMINANCE HELD':'NO STREAK');set(id+'StreakDetail',t?'Observed dominance '+fmtDuration(t)+' ('+frameName(s.sec)+')':'No confirmed dominance');$(id+'Meter').style.width=Math.min(100,t/180*100)+'%';$(id+'Tile').dataset.level=t>=180?4:t>=60?3:t>=25?2:t>0?1:0}
 set('forecastDirection',o.ready?(o.outlook.side==='ABSTAIN'?'WAIT / NO EDGE':o.outlook.side+' PRESSURE POSSIBLE'):'WAIT · '+o.status);
 $('forecastDirection').className=o.ready&&o.outlook.side==='BUY'?'positive':o.ready&&o.outlook.side==='SELL'?'negative':'neutral';
 set('forecastReason',o.ready?o.outlook.reason:'Awaiting sufficient '+frameName(o.sec)+' observations');
 set('forecastTrack',frameName(o.sec)+' / '+quote+' context');
 if(!$('framePicks').children.length)$('framePicks').innerHTML=FRAMES.map(sec=>'<label class="frame-chip"><input type="checkbox" data-frame="'+sec+'" '+(selected.has(sec)?'checked':'')+'>'+frameName(sec)+'</label>').join('');
 else for(const input of $('framePicks').querySelectorAll('[data-frame]'))input.checked=selected.has(+input.dataset.frame);
 const chosen=[...selected].sort((a,b)=>a-b).map(sec=>map.get(sec));
 $('frameRows').innerHTML=chosen.map(x=>{
 const streak=x.ready&&x.state.side!=='NONE'?x.state.side+' '+fmtDuration(Math.floor((now-x.state.start)/1000)):'—',momentum=x.momentum==null?'—':(x.momentum>=0?'+':'')+x.momentum.toFixed(1)+'pp';
 return '<div class="upgrade-row"><b>'+frameName(x.sec)+'</b><div><span class="positive">'+(x.ready?percent(x.w.buyPercent):'—')+'</span> <span class="negative">'+(x.ready?percent(100-x.w.buyPercent):'—')+'</span><div class="upbar"><i style="width:'+(x.ready?x.w.buyPercent:0)+'%"></i></div></div><div>'+(!x.ready?'—':x.direction.replace('_',' '))+'</div><div>'+momentum+'</div><div>'+streak+'</div><div>'+(!x.ready?'WAIT':x.outlook.side==='ABSTAIN'?'NO EDGE':x.outlook.side+'?')+'</div><small>'+(x.ready?'QUALIFIED':x.status+' '+x.progress+'%')+'</small></div>'
 }).join('')||'<p class="empty">Select at least one timeframe</p>';
 const c=E.consensus(chosen.map(x=>({ready:x.ready,share:x.w.buyPercent,direction:x.direction})));
 const aligned=c.status==='ALIGNED',direction=c.status==='CONFLICT'?'CONFLICT · WITHHOLD SIGNAL':c.status==='WARMUP'?'WAIT / MORE HISTORY':aligned?c.leader+' DOMINANCE CONFIRMED':'MIXED / NEUTRAL';
 set('bossRegime',direction);set('bossDominance',direction);$('bossRegime').className=$('bossDominance').className=aligned?(c.leader==='BUY'?'positive':'negative'):'neutral';
 set('bossBuyPct',aligned?percent(c.share):'—');set('bossSellPct',aligned?percent(100-c.share):'—');$('bossPressureFill').style.width=(aligned?c.share:0)+'%';
 set('bossPressureStatus',c.status);set('bossAgreement',c.alignment==null?'—':Math.round(c.alignment)+'%');set('bossCoverage',c.qualified+' / '+c.selected);set('bossReliability',c.status);set('bossWindowCount',c.qualified+' / '+c.selected+' qualified');set('bossConflict',c.alignment==null?'Unverified':Math.round(c.alignment)+'% aligned');
 set('bossRawMean','Descriptive mean: '+(c.mean==null?'—':percent(c.mean))+' · not a signal');
 const explanation=c.status==='CONFLICT'?'Timeframes disagree; no directional master reading.':c.status==='WARMUP'?'Need two complete qualified windows.':aligned?'Observed aligned aggressor flow; price confirmation is separate.':'Weak or mixed directional agreement.';
 set('bossReason',explanation);set('bossPressureExplain',explanation);set('bossBuy',c.buys+' BUY');set('bossSell',c.sells+' SELL');$('bossFill').style.width=(aligned?c.share:0)+'%';
 const votes=chosen.filter(x=>x.ready&&x.outlook.side!=='ABSTAIN'),vb=votes.filter(x=>x.outlook.side==='BUY').length,vs=votes.filter(x=>x.outlook.side==='SELL').length;
 const good=votes.length>=2&&Math.max(vb,vs)>=2&&!(vb&&vs)&&Math.max(vb,vs)/votes.length>=.7;
 set('bossOutlook',good?(vb>vs?'BUY':'SELL')+' PRESSURE POSSIBLE':'WAIT / NO EDGE');
 $('bossOutlook').className=good?(vb>vs?'positive':'negative'):'neutral';
 set('bossForecastReason',good?'Correlated timeframe contexts agree. Price confirmation required.':'No verified directional agreement across complete windows.');
 return {chosen,p,c};
}
function render(){
 if(document.hidden)return;const now=Date.now();
 for(const side of ['USD','USDT'])for(const sec of buckets[side].keys())if(sec<Math.floor((now-3660000)/1000))buckets[side].delete(sec);
 research(now);
 const w=windowData(60,now).w,ref=references[quote],venue=quote==='USDT'?'Binance':'Coinbase',fresh=ref&&now-ref.receivedAt<9000;
 const active=VENUES.filter(n=>feeds[n]?.lastTrade&&now-feeds[n].lastTrade<15000).length,connected=VENUES.filter(n=>feeds[n]?.ws?.readyState===WebSocket.OPEN).length;
 $('statusLed').classList.toggle('active',active>0);set('connectionState',active+' RECEIVING · '+connected+' CONNECTED');
 set('referencePrice',fresh?fmtPrice(ref.price)+' '+quote:'—');set('referenceLabel',venue+' · '+asset+'/'+quote+' spot, not consolidated');set('referenceAge',fresh?'Trade '+fmtLocal(ref.time)+' · '+Math.round((now-ref.receivedAt)/1000)+'s ago':'No fresh '+venue+' reference price');
 set('referenceLatency',fresh?Math.max(0,Math.round(ref.receivedAt-ref.time))+'ms':'—');
 set('referenceSkew',fresh&&ref.receivedAt<ref.time?' · negative clock skew':'');
 set('buyNotional',money(w.buy));set('sellNotional',money(w.sell));set('netDelta',w.count?(w.buy>=w.sell?'+':'−')+money(Math.abs(w.buy-w.sell)):'—');
 set('sampleInfo',w.count.toLocaleString()+' prints / '+w.venueCount+' '+quote+' venues · 60s');
 set('timezone',Intl.DateTimeFormat().resolvedOptions().timeZone||'UTC');
 set('symbolCode',asset+'/'+quote+' · '+venue+' spot');
 const f=renderFrames(now);
 const last=summarize(10,now),before=E.summaryBins(buckets[quote],Math.floor(now/1000)*1000-20000,Math.floor(now/1000)*1000-10000);
 set('trend30',percent(summarize(30,now).buyPercent));set('trend60',percent(w.buyPercent));
 set('forecastCountdown',pending?Math.max(0,Math.ceil((pending.until-now)/1000))+'s':'—');
 const valid=results.filter(x=>x.asset===asset&&x.quote===quote),flow=E.benchmark(valid),priceRecords=valid.filter(x=>x.priceResult==='CORRECT'||x.priceResult==='WRONG'),priceHits=priceRecords.filter(x=>x.priceResult==='CORRECT').length;
 const priceCI=E.wilson(priceHits,priceRecords.length);set('empiricalRate',flow.total>=30?flow.interval.rate.toFixed(1)+'% · '+flow.total+' forward samples':'UNVERIFIED · '+flow.total+'/30 samples');
 set('forecastStatus',flow.total+' scored future windows');set('flowHitRate',flow.total?flow.interval.rate.toFixed(1)+'% ('+flow.total+') · CI '+flow.interval.low.toFixed(0)+'–'+flow.interval.high.toFixed(0)+'%':'—');
 set('priceHitRate',priceCI?priceCI.rate.toFixed(1)+'% ('+priceRecords.length+')':'—');
 set('benchmarkRate',flow.baseline==null?'—':'Always-majority baseline '+flow.baseline.toFixed(1)+'%');set('priceAccuracy',priceCI&&priceRecords.length>=30?priceCI.rate.toFixed(1)+'% / '+priceRecords.length:'Unverified · '+priceRecords.length+'/30');
 set('validationCount',flow.total>=100?'Research sample only · not an out-of-sample guarantee':flow.total+' / 100 desired checks (collecting)');
 set('lastOutcome',lastResult?lastResult.side+' PRESSURE: '+lastResult.flowResult:'No completed check');set('outcomeDetails',lastResult?'Δ flow '+(lastResult.flowDelta==null?'—':lastResult.flowDelta.toFixed(2)+'pp')+' · price '+(lastResult.returnPct==null?'—':lastResult.returnPct.toFixed(3)+'%'):'Waiting for future execution');
 set('observations',valid.length+' verified or invalid labels');
 set('totalReceived',total.toLocaleString()+' received');set('bufferSize',raw.length+' stored · newest 75 displayed');
 set('lastVenue',lastPrint?lastPrint.venue+' '+lastPrint.asset+'/'+lastPrint.quote+' · '+fmtLocal(lastPrint.time):'No verified trade');
 const refSeries=series[quote].filter(x=>x.t>=now-30000).sort((a,b)=>a.t-b.t),old=refSeries[0],latest=refSeries.at(-1);
 const ret=old&&latest?100*(latest.p/old.p-1):null;
 set('priceReturn',ret==null?'—':(ret>=0?'+':'')+ret.toFixed(3)+'%');
 const delta=last.buyPercent==null||before.buyPercent==null?null:last.buyPercent-before.buyPercent;
 const resp=ret==null||w.count<40?'INSUFFICIENT DATA':Math.abs(ret)<.015&&Math.abs(delta||0)>5?'POSSIBLE ABSORPTION':ret>0&&w.buyPercent>=55?'BUY PRESSURE CONFIRMED':ret<0&&w.buyPercent<=45?'SELL PRESSURE CONFIRMED':'FLOW / PRICE DIVERGENCE';
 set('priceResponse',resp);
 set('responseDescription','Pressure '+percent(w.buyPercent)+' vs same-venue 30s price change '+(ret==null?'—':ret.toFixed(3)+'%')+'. No causality inferred.');
 set('activeVenues',freshVenues(now).length+' / '+VENUES.filter(n=>marketQuote(n)===quote).length);
 const conc=E.concentration(w);set('venueConcentration',conc==null?'—':percent(100*conc));
 const venueList=[...w.byVenue.entries()].sort((a,b)=>(b[1].buy+b[1].sell)-(a[1].buy+a[1].sell));
 $('venueAttribution').innerHTML=venueList.length?venueList.map(([name,v])=>'<div class="attribution-row"><strong>'+name+'</strong><span>'+money(v.buy)+' buy</span><span>'+money(v.sell)+' sell</span><span>'+percent(100*(v.buy+v.sell)/Math.max(1,w.buy+w.sell))+' share</span></div>').join(''):'<p class="muted">Awaiting observations from '+quote+' markets.</p>';
 $('venueRows').innerHTML=VENUES.map(n=>{const v=feeds[n],age=v.lastTrade?Math.floor((now-v.lastTrade)/1000)+'s':'never',fresh=v.lastTrade&&now-v.lastTrade<15000;
 return '<div class="venue-row"><span><i class="led-small '+(fresh?'good':'')+'"></i><b>'+n+'</b><small>'+asset+'/'+marketQuote(n)+'</small></span><span class="venue-right">'+(v.disabled?'UNSUPPORTED':fresh?'RECEIVING':v.status)+'<small>'+v.count.toLocaleString()+' prints · '+age+' · '+(v.error||'')+'</small></span></div>'}).join('');
 const filter=$('filterVenue').value,side=$('filterSide').value,min=Math.max(0,Number($('minTrade').value)||0);
 if(now-lastTape>=2200||displayVersion!==total){lastTape=now;displayVersion=total;
 const matches=raw.filter(t=>(filter==='ALL'||filter===t.venue)&&(side==='ALL'||side===t.side)&&t.value>=min).slice(-260).sort((a,b)=>b.time-a.time).slice(0,75);
 $('tapeRows').innerHTML=matches.length?matches.map(t=>'<tr><td>'+fmtLocal(t.time)+'</td><td>'+t.venue+'<small>'+t.quote+'</small></td><td class="'+(t.side==='BUY'?'positive':'negative')+'">'+t.side+'</td><td>'+fmtPrice(t.price)+' '+t.quote+'</td><td>'+money(t.value)+'</td></tr>').join(''):'<tr><td colspan="5" class="empty">No matching genuine executions received</td></tr>'; }
 $('historyRows').innerHTML=valid.slice(-12).reverse().map(x=>'<tr><td>'+fmtLocal(x.issuedAt)+'</td><td>'+x.side+'</td><td>'+x.flowResult+'</td><td>'+(x.flowDelta==null?'—':x.flowDelta.toFixed(1)+'pp')+' / '+(x.returnPct==null?'—':x.returnPct.toFixed(3)+'%')+'</td></tr>').join('')||'<tr><td colspan="4" class="empty">No scored forward samples yet</td></tr>';
 drawChart(now);
}
document.querySelectorAll('[data-symbol]').forEach(b=>b.addEventListener('click',()=>{asset=b.dataset.symbol;try{localStorage.setItem(selectedKey,asset)}catch{}resetMarket()}));
$('quoteSelect').addEventListener('change',e=>{quote=e.target.value;quoteEpoch=Date.now();pending=null;lastIssued=0;frameStreaks=Object.fromEntries(FRAMES.map(s=>[s,{side:'NONE',candidate:0,since:0}]));render()});
for(const id of ['pressureFrame','streakFrame','outlookFrame'])$(id).addEventListener('change',render);
$('framePicks').addEventListener('change',e=>{if(!e.target.matches('[data-frame]'))return;const s=Number(e.target.dataset.frame);if(e.target.checked)selected.add(s);else selected.delete(s);render()});
$('selectAllFrames').addEventListener('click',()=>{selected=new Set(FRAMES);render()});
$('focusFrames').addEventListener('click',()=>{selected=new Set([30,60,300]);render()});
$('reconnect').addEventListener('click',()=>{for(const n of VENUES)if(!feeds[n].disabled)connect(n);render()});
['filterVenue','filterSide','minTrade'].forEach(id=>$(id).addEventListener('input',()=>{lastTape=0;displayVersion=-1;render()}));
$('clearOutcomes').addEventListener('click',()=>{results=results.filter(x=>x.asset!==asset||x.quote!==quote);save();render()});
document.querySelectorAll('.section-nav a').forEach(el=>el.addEventListener('click',()=>{document.querySelectorAll('.section-nav a').forEach(a=>a.classList.toggle('nav-active',a===el))}));
window.addEventListener('resize',()=>{lastChart=0;drawChart(Date.now())});
document.addEventListener('visibilitychange',()=>{if(document.hidden){pending=null}else{quoteEpoch=Date.now();watchdog();render()}});
window.addEventListener('online',watchdog);
set('timezone',Intl.DateTimeFormat().resolvedOptions().timeZone||'UTC');
resetMarket();setInterval(()=>{if(!document.hidden)render()},1000);setInterval(watchdog,7500);
})();