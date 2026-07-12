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

const server = http.createServer((req, res) => {
  const u = new URL(req.url, "http://localhost");

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
