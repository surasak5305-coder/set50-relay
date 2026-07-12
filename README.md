# set50-relay

Intraday drop-box for the SET50 Options dashboard — each PC pushes its live
payoff/positions here; other PCs pull to view. Nothing persists: an account that
stops pushing (M2M Save / app closed) is deleted automatically. See the deploy
steps in the main project notes (Render web service, env `RELAY_KEY`).

- `POST /api/ingest` (header `x-api-key: <RELAY_KEY>`) — body `{id, ...state}` or `{id, offline:true}`
- `GET /api/data` (same key) — `{accounts:[{id, online, lastBeat, ...state}]}`
- `GET /` — health check (no key)

Run locally: `set RELAY_KEY=test && node server.js` (port 10777, or `PORT` env).
