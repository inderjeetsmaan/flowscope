# FlowScope

**Live trades, market pressure and a closer look at the next ten seconds.**

FlowScope is a lightweight market-research dashboard for watching public cryptocurrency trade executions across multiple exchanges. The idea is simple: one clean screen where you can see incoming buys and sells, compare aggressive volume, and follow whether pressure is building or fading.

**Live site:** [FlowScope on Vercel](https://flowscope-live-tape-maan-0a06.vercel.app)

## Preview

![Desktop overview](docs/screenshots/desktop.png)

<p align="center"><img src="docs/screenshots/mobile.png" alt="FlowScope mobile dashboard" width="320"></p>

The screenshots show the interface in a disconnected preview environment. Figures are intentionally empty until a live exchange feed connects. The application never fabricates market trades.

## What it does

- Watches individual public trade prints from Binance, Coinbase, Kraken, OKX, Bybit, Gate.io, Bitstamp and Bitget when their public WebSocket markets are available.
- Supports BTC, ETH, SOL, and PAXG (tokenized gold) spot pairs.
- Calculates rolling buyer-initiated vs seller-initiated traded notional over 15 seconds, 30 seconds, 1 minute or 5 minutes.
- Separates **connected** sockets from venues that are actually **receiving** trades.
- Shows growing green/red strength indicators when a smoothed pressure trend persists, with confirmation periods to filter small changes.
- Records 10-second pressure outcomes and compares them with similar past conditions. An estimated continuation rate is withheld until at least 20 comparable verified checks exist.
- Lets you filter the latest execution tape by exchange, aggressor side and minimum trade value.
- Keeps a small amount of verified outcome history locally in the browser.

## Run locally

No installation, account or API key is needed. This is a static HTML/CSS/JavaScript project.

```bash
python -m http.server 8080
```

Open `http://localhost:8080`. Use any standard static file server or deploy the folder to Vercel. The site opens WebSocket connections directly from the browser.

## How to read the pressure

A public trade has both a buyer and seller. A **buy** print in FlowScope refers to the more aggressive participant crossing the spread to buy; it does not mean there are more buyers than sellers. For the selected window:

```text
Buy pressure (%) = buyer-initiated notional / all observed notional × 100
Net taker delta  = buyer-initiated notional − seller-initiated notional
```

A green or red timer measures how long a *filtered increase in that side's pressure* has persisted. The effect strengthens with the duration of the streak; it does **not** mean a prediction is becoming certain. Short-lived changes are ignored using exponential smoothing, a threshold and a confirmation period.

The 10-second outlook studies similar observed conditions and labels whether pressure increased, decreased or stayed roughly flat after ten seconds. It is intentionally cautious about sparse samples. That model has not been independently backtested against an external benchmark and must not be treated as a live trading recommendation.

## Project structure

```text
flowscope/
├── index.html             # Page structure
├── src/
│   ├── styles.css         # Responsive design and pressure effects
│   └── app.js             # Exchange streams, aggregation and outlooks
├── docs/screenshots/
│   ├── desktop.png
│   └── mobile.png
├── vercel.json
├── .gitignore
└── README.md
```

## Things worth knowing

- A browser-only application cannot capture every global transaction. Some markets are private, do not publish individual executions, or restrict direct WebSocket access.
- Spot BTC/USD and BTC/USDT are different quote markets; summed USD/USDT notional is approximate, not a perfectly normalized global tape.
- **PAXG is not XAU/USD.** Tokenized-gold exchange prints should not be used as broker gold order flow.
- A phone can suspend an inactive page. No new trades are collected while the browser is suspended. Live monitoring resumes after reconnection, but the gap is not backfilled.
- The dashboard does **not** execute trades, connect to a brokerage account, or handle funds.

## About

FlowScope is an ongoing independent market-data interface project maintained by **Inderjeet Singh Maan**. The focus is clear presentation, practical live exchange monitoring and transparent limitations around market-pressure signals.