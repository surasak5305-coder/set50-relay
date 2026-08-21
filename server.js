/* SET50 relay — intraday state drop-box for the Options dashboard (multi-PC / multi-home)
 *
 * Pattern copied from C:\dashboard server.js (OrderFlow Monitor) and trimmed:
 *   PC (each account) --POST /api/ingest--> writes data/account_<id>.json  (x-api-key)
 *   other PCs         --GET  /api/data --> all accounts + online/lastBeat  (x-api-key)
 *
 * Intraday ONLY — nothing survives the day by design:
 *   - a PC that stops pushing goes offline in OFFLINE_SECONDS and its file is
 *     DELETED after REMOVE_SECONDS (M2M Save pushes {offline:true} = gone at once)
 *   - Render free tier disk is ephemeral anyway (restart wipes ./data) — fine here
 *
 * Auth: single env RELAY_KEY guards both push and pull (keep it secret).
 * No dependencies — Node built-ins only. Start: node server.js
 */
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const crypto = require("crypto");

const DATA_DIR = path.join(__dirname, "data");
const KEY = process.env.RELAY_KEY || "";
const PORT = process.env.PORT || 10777;
const OFFLINE_MS = (Number(process.env.OFFLINE_SECONDS) || 120) * 1000;
const REMOVE_MS = (Number(process.env.REMOVE_SECONDS) || 300) * 1000;
const MAX_BODY = 2 * 1024 * 1024;
const VALID_ID = /^[\w.-]+$/;

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

function agoStr(ms) {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return s + "s";
  const m = Math.round(s / 60);
  return m < 60 ? m + "m" : Math.round(m / 60) + "h";
}

/* read every account_*.json; auto-delete stale ones (stopped pushing) */
function readFolder() {
  const now = Date.now();
  const accounts = [];
  let files = [];
  try { files = fs.readdirSync(DATA_DIR).filter((f) => f.endsWith(".json")); } catch {}
  for (const f of files) {
    const full = path.join(DATA_DIR, f);
    let obj;
    try { obj = JSON.parse(fs.readFileSync(full, "utf8")); } catch { continue; }
    if (!obj || obj.id == null) continue;
    const lastSeen = obj._lastSeen || 0;
    const age = now - lastSeen;
    if (age >= REMOVE_MS) { try { fs.unlinkSync(full); } catch {} continue; }
    obj.online = age < OFFLINE_MS;
    obj.lastBeat = agoStr(age);
    delete obj._lastSeen;
    accounts.push(obj);
  }
  accounts.sort((a, b) => String(a.id).localeCompare(String(b.id)));
  return { accounts, updatedAt: now };
}

function sendJSON(res, code, obj, headers) {
  res.writeHead(code, Object.assign(
    { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" }, headers || {}));
  res.end(JSON.stringify(obj));
}

/* ETag from stable fields only (online/lastBeat/updatedAt change every call) +
   gzip — same bandwidth savers as OrderFlow (/api/data is polled every few s) */
function sendData(req, res, folder) {
  const stable = folder.accounts.map((a) => { const { online, lastBeat, ...rest } = a; return rest; });
  const etag = '"' + crypto.createHash("sha1").update(JSON.stringify(stable)).digest("hex") + '"';
  const base = { "Cache-Control": "no-cache", "ETag": etag, "Vary": "Accept-Encoding" };
  if (req.headers["if-none-match"] === etag) { res.writeHead(304, base); return res.end(); }
  const json = JSON.stringify(folder);
  const headers = Object.assign({ "Content-Type": "application/json; charset=utf-8" }, base);
  if (/\bgzip\b/.test(String(req.headers["accept-encoding"] || ""))) {
    headers["Content-Encoding"] = "gzip";
    res.writeHead(200, headers);
    return res.end(zlib.gzipSync(json));
  }
  res.writeHead(200, headers);
  return res.end(json);
}

function authed(req, u) {
  if (!KEY) return true;                      // no key set = open (dev only; always set on Render)
  return req.headers["x-api-key"] === KEY || u.searchParams.get("key") === KEY;
}

/* ══════════ mobile settle monitor ══════════
 * PC pushes an evening bundle (positions+payoff, marks at close or official settle)
 * to GitHub repo set50-ledger as mobile/<account>.json (see backend/settle_push.py).
 * The phone opens /view here: bundle comes from GitHub (private -> GH_TOKEN env),
 * official settlement comes from the public tfex.co.th JSON endpoints (no login;
 * same Incapsula cookie dance as backend/tfex_settle.py). On-access only, no cron.
 */
const GH_TOKEN = process.env.RELAY_GH_TOKEN || "";
const GH_REPO = process.env.RELAY_GH_REPO || "surasak5305-coder/set50-ledger";
const TFEX_PAGE = "https://www.tfex.co.th/en/products/equity/set50-index-options/market-data";
const TFEX_API = "https://www.tfex.co.th/api/set/tfex";
const TFEX_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const SETTLE_REFETCH_MS = 5 * 60 * 1000;   // not-final -> retry at most every 5 min
const MONTH_CODE = { F: 1, G: 2, H: 3, J: 4, K: 5, M: 6, N: 7, Q: 8, U: 9, V: 10, X: 11, Z: 12 };

let tfexCookie = "";                        // Incapsula session cookies
const settleCache = {};                     // series -> {res, at, finalDate}
const bundleCache = {};                     // acct -> {res, at}
const acctsCache = { at: 0, res: null };    // /api/accounts -> {ok, accounts:[...]}

function tfexNum(v) {
  if (v == null || v === "" || v === "-") return null;
  const n = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

async function tfexGet(path, retry) {
  if (!tfexCookie) {
    const p = await fetch(TFEX_PAGE, { headers: { "User-Agent": TFEX_UA } });
    const sc = (p.headers.getSetCookie ? p.headers.getSetCookie() : []) || [];
    tfexCookie = sc.map((c) => c.split(";")[0]).join("; ");
  }
  const r = await fetch(TFEX_API + path, {
    headers: { "User-Agent": TFEX_UA, "Referer": TFEX_PAGE,
      "Accept": "application/json", "Cookie": tfexCookie },
  });
  if (r.status === 200) return r.json();
  tfexCookie = "";                          // kicked (403/503) -> new cookies, one retry
  if (!retry && [401, 403, 429, 503].includes(r.status)) return tfexGet(path, true);
  throw new Error("tfex HTTP " + r.status);
}

/* same shape/logic as backend/tfex_settle.fetch(): latest *published* settle,
 * todayPublished tells whether it is today's (else prior trading day, always final) */
async function fetchSettle(series) {
  const tail = series.slice(-3);
  const cm = String(MONTH_CODE[tail[0]]).padStart(2, "0") + "/20" + tail.slice(1);
  const fut = await tfexGet("/series/" + series + "/info");
  let asof, futSettle, useToday;
  if (tfexNum(fut.settlementPrice) != null) {
    asof = String(fut.settlementDate || "").slice(0, 10) || new Date().toISOString().slice(0, 10);
    futSettle = tfexNum(fut.settlementPrice);
    useToday = true;
  } else {
    asof = String(fut.priorSettlementDate || "").slice(0, 10);
    futSettle = tfexNum(fut.priorSettlementPrice);
    useToday = false;
  }
  if (!asof || futSettle == null) throw new Error("no settle on futures info");
  const legs = { FUT: futSettle };
  try {
    const chain = await tfexGet("/marketlist/TXI_O/options-trading");
    const cms = ((chain.instruments || [])[0] || {}).contractMonths || [];
    const block = cms.find((c) => c.contractMonth === cm) || {};
    for (const row of block.callPutList || []) {
      for (const [cp, side] of [["C", "call"], ["P", "put"]]) {
        const v = tfexNum((row[side] || {})[useToday ? "settlementPrice" : "priorSettlementPrice"]);
        if (v != null) legs[parseInt(row.strikePrice, 10) + "-" + cp] = v;
      }
    }
  } catch { /* futures settle alone still useful */ }
  return { ok: true, series, asof, todayPublished: useToday, final: true,
    legs, fetchedAt: new Date().toISOString().slice(0, 19) };
}

async function settleHandler(res, series) {
  if (!/^[A-Z0-9]{3,10}$/.test(series)) return sendJSON(res, 400, { ok: false, error: "bad series" });
  const today = new Date().toISOString().slice(0, 10);
  const c = settleCache[series];
  // published-today result is final -> serve forever (per-day); otherwise throttle refetch
  if (c && (c.finalDate === today || Date.now() - c.at < SETTLE_REFETCH_MS)) {
    return sendJSON(res, 200, Object.assign({ source: "cache" }, c.res));
  }
  try {
    const out = await fetchSettle(series);
    settleCache[series] = { res: out, at: Date.now(),
      finalDate: out.todayPublished && out.asof === today ? today : "" };
    return sendJSON(res, 200, out);
  } catch (e) {
    if (c) return sendJSON(res, 200, Object.assign({ source: "stale-cache" }, c.res));
    return sendJSON(res, 502, { ok: false, error: String(e.message || e) });
  }
}

/* A fine-grained token must carry an expiry date (GitHub caps it at ~1 year), so this
 * WILL start failing one day. Say which of the three causes it is instead of leaving a
 * bare "HTTP 401" on the phone — the same silent death as an expired token in .git/config. */
function ghErr(status) {
  if (status === 401) return "GitHub token expired or revoked — renew RELAY_GH_TOKEN on Render";
  if (status === 403) return "GitHub refused — token has no Contents:read on the ledger repo (or rate-limited)";
  return "github HTTP " + status;
}

/* one GitHub Contents API call (private repo -> needs the token) */
function ghGet(p, raw) {
  return fetch("https://api.github.com/repos/" + GH_REPO + p, {
    headers: { "Authorization": "Bearer " + GH_TOKEN, "User-Agent": "set50-relay",
      "Accept": raw ? "application/vnd.github.raw+json" : "application/vnd.github+json" },
  });
}

/* Account picker for /view — the phone's version of the [Payoff][v] menu on the
 * dashboard's Position Summary card. Every account that ever pushed a bundle has a
 * file in mobile/, so listing that folder IS the account list (one API call); the
 * label fields (profile/series/date) live inside each bundle, which we fetch once
 * and park in bundleCache -> switching to that account afterwards costs nothing.
 * Cached 5 min: bundles change once a day (after the close), never intraday. */
const ACCTS_TTL = 5 * 60 * 1000;
async function accountsHandler(res) {
  if (!GH_TOKEN) return sendJSON(res, 500, { ok: false, error: "RELAY_GH_TOKEN not set" });
  if (acctsCache.res && Date.now() - acctsCache.at < ACCTS_TTL) {
    return sendJSON(res, 200, acctsCache.res);
  }
  try {
    const r = await ghGet("/contents/mobile?ref=main");
    if (r.status === 404) return sendJSON(res, 200, { ok: true, accounts: [] });
    if (r.status !== 200) return sendJSON(res, 502, { ok: false, error: ghErr(r.status) });
    const files = await r.json();
    const names = (Array.isArray(files) ? files : [])
      .filter((f) => f.type === "file" && f.name.endsWith(".json") && !f.name.startsWith("_"))
      .map((f) => f.name.slice(0, -5))
      .filter((n) => VALID_ID.test(n));
    const accounts = [];
    for (const acct of names) {
      let b = bundleCache[acct] && bundleCache[acct].res;
      if (!b) {
        const br = await ghGet("/contents/mobile/" + encodeURIComponent(acct) + ".json?ref=main", true);
        if (br.status !== 200) continue;      // skip a file we cannot read, list the rest
        b = await br.json();
        bundleCache[acct] = { res: b, at: Date.now() };
      }
      accounts.push({ acct, profile: b.profile || "", series: b.series || "",
        date: b.date || "", settled: !!b.settled, generatedAt: b.generatedAt || "" });
    }
    // newest bundle first, then by name -> the account you are most likely after is on top
    accounts.sort((a, b) => (a.date < b.date ? 1 : a.date > b.date ? -1 : a.acct < b.acct ? -1 : 1));
    acctsCache.res = { ok: true, accounts };
    acctsCache.at = Date.now();
    return sendJSON(res, 200, acctsCache.res);
  } catch (e) {
    return sendJSON(res, 502, { ok: false, error: String(e.message || e) });
  }
}

async function bundleHandler(res, acct) {
  if (!VALID_ID.test(acct)) return sendJSON(res, 400, { ok: false, error: "bad acct" });
  if (!GH_TOKEN) return sendJSON(res, 500, { ok: false, error: "RELAY_GH_TOKEN not set" });
  const c = bundleCache[acct];
  if (c && Date.now() - c.at < 60 * 1000) return sendJSON(res, 200, c.res);
  try {
    const r = await fetch("https://api.github.com/repos/" + GH_REPO + "/contents/mobile/" +
      encodeURIComponent(acct) + ".json?ref=main", {
      headers: { "Authorization": "Bearer " + GH_TOKEN,
        "Accept": "application/vnd.github.raw+json", "User-Agent": "set50-relay" },
    });
    if (r.status === 404) return sendJSON(res, 404, { ok: false, error: "no bundle for " + acct });
    if (r.status !== 200) return sendJSON(res, 502, { ok: false, error: ghErr(r.status) });
    const out = await r.json();
    bundleCache[acct] = { res: out, at: Date.now() };
    return sendJSON(res, 200, out);
  } catch (e) {
    return sendJSON(res, 502, { ok: false, error: String(e.message || e) });
  }
}

const VIEW_HTML = fs.readFileSync(path.join(__dirname, "view.html"));
const VIEW_ICONS = {
  "/view-icon-192.png": fs.readFileSync(path.join(__dirname, "view-icon-192.png")),
  "/view-icon-512.png": fs.readFileSync(path.join(__dirname, "view-icon-512.png")),
};
/* own manifest -> "Add to Home Screen" installs /view as its own app + icon,
   separate from the dashboard PWA. start_url has no params: the page keeps
   acct/key in localStorage from the first parameterized visit. */
const VIEW_MANIFEST = JSON.stringify({
  name: "SET50 Settle Monitor", short_name: "STL", start_url: "/view",
  display: "standalone", background_color: "#0d1520", theme_color: "#0d1520",
  icons: [
    { src: "/view-icon-192.png", sizes: "192x192", type: "image/png" },
    { src: "/view-icon-512.png", sizes: "512x512", type: "image/png" },
  ],
});

const server = http.createServer((req, res) => {
  const u = new URL(req.url, "http://localhost");

  // mobile settle monitor page + its data endpoints
  if (req.method === "GET" && VIEW_ICONS[u.pathname]) {
    res.writeHead(200, { "Content-Type": "image/png", "Cache-Control": "public, max-age=86400" });
    return res.end(VIEW_ICONS[u.pathname]);
  }
  if (req.method === "GET" && u.pathname === "/view.webmanifest") {
    res.writeHead(200, { "Content-Type": "application/manifest+json",
      "Cache-Control": "public, max-age=86400" });
    return res.end(VIEW_MANIFEST);
  }
  if (req.method === "GET" && u.pathname === "/view") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
    return res.end(VIEW_HTML);
  }
  if (req.method === "GET" && u.pathname === "/api/settle") {
    return void settleHandler(res, String(u.searchParams.get("series") || "").toUpperCase());
  }
  if (req.method === "GET" && u.pathname === "/api/bundle") {
    if (!authed(req, u)) return sendJSON(res, 401, { ok: false, error: "unauthorized" });
    return void bundleHandler(res, String(u.searchParams.get("acct") || ""));
  }
  if (req.method === "GET" && u.pathname === "/api/accounts") {
    if (!authed(req, u)) return sendJSON(res, 401, { ok: false, error: "unauthorized" });
    return void accountsHandler(res);
  }

  // PC pushes its intraday state here (payoff+positions, ~10-20 KB)
  if (req.method === "POST" && u.pathname === "/api/ingest") {
    if (!authed(req, u)) return sendJSON(res, 401, { ok: false, error: "bad api key" });
    let body = "";
    let tooBig = false;
    req.on("data", (c) => { body += c; if (body.length > MAX_BODY) { tooBig = true; req.destroy(); } });
    req.on("end", () => {
      if (tooBig) return;
      let obj;
      try { obj = JSON.parse(body); } catch { return sendJSON(res, 400, { ok: false, error: "invalid json" }); }
      const id = String(obj && obj.id != null ? obj.id : "");
      if (!VALID_ID.test(id)) return sendJSON(res, 400, { ok: false, error: "bad id" });
      const full = path.join(DATA_DIR, "account_" + id + ".json");
      if (obj.offline === true) {              // M2M Save / shutdown → gone immediately
        try { fs.unlinkSync(full); } catch {}
        return sendJSON(res, 200, { ok: true, removed: id });
      }
      obj._lastSeen = Date.now();
      try { fs.writeFileSync(full, JSON.stringify(obj)); } catch (e) {
        return sendJSON(res, 500, { ok: false, error: String(e.message || e) });
      }
      return sendJSON(res, 200, { ok: true, id });
    });
    return;
  }

  // other PCs pull everything here
  if (req.method === "GET" && u.pathname === "/api/data") {
    if (!authed(req, u)) return sendJSON(res, 401, { ok: false, error: "unauthorized" });
    return sendData(req, res, readFolder());
  }

  // health/status (no key needed, leaks nothing but a count)
  if (req.method === "GET" && u.pathname === "/") {
    return sendJSON(res, 200, { ok: true, service: "set50-relay", accounts: readFolder().accounts.length });
  }

  sendJSON(res, 404, { ok: false, error: "not found" });
});

server.listen(PORT, () => {
  console.log("set50-relay listening on :" + PORT);
  if (!KEY) console.log("WARNING: RELAY_KEY is empty - anyone can push/read. Set it in env.");
});
