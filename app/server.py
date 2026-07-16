#!/usr/bin/env python3
"""
s1-ueba-deployer, interactive UI (option 1).

Zero-dependency local web server. Reads S1 credentials from environment variables
(or the local Claude Desktop config as a fallback) via core.py, serves the one-click
UEBA deployment UI, and proxies every S1 API call server-side so no token ever reaches
the browser.

Run:
  export S1_CONSOLE_URL=...     S1_CONSOLE_API_TOKEN=...
  export SDL_XDR_URL=...        SDL_CONFIG_WRITE_KEY=... SDL_CONFIG_READ_KEY=...
  export S1_HEC_INGEST_URL=...   # HEC ingest uses S1_CONSOLE_API_TOKEN
  python app/server.py
Then open http://localhost:8788
"""
import os, sys, json, http.server, socketserver, pathlib
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import core

HERE = pathlib.Path(__file__).resolve().parent
# Distinct default port from s1-ueba-deployer (8799) so both can run side by side.
PORT = int(os.environ.get("INGEST_PORT", os.environ.get("UEBA_PORT", "8788")))
HOST = os.environ.get("INGEST_HOST", os.environ.get("UEBA_HOST", "127.0.0.1"))
# Extra exact origins (comma-separated) for hosted setups; local use needs none.
_EXTRA_ORIGINS = {o.strip() for o in os.environ.get("UEBA_ALLOWED_ORIGINS", "").split(",") if o.strip()}

# --- Network-exposure controls -------------------------------------------------------------------
# This tool drives privileged S1 API calls with the configured token, so an open, unauthenticated
# port is a real risk. Two modes:
#   * Local (default): meant to be reached only from the same machine. In Docker, publish to the
#     host loopback: `-p 127.0.0.1:8888:8788`. No token needed.
#   * Exposed: set INGEST_BIND_ALL=1 to intentionally serve to other hosts. In that mode an
#     INGEST_AUTH_TOKEN is MANDATORY and is required on every /api call (header X-Ingest-Auth or
#     ?token=). The server refuses to start exposed without one.
AUTH_TOKEN = os.environ.get("INGEST_AUTH_TOKEN", os.environ.get("UEBA_AUTH_TOKEN", "")).strip()
EXPOSED = os.environ.get("INGEST_BIND_ALL", os.environ.get("UEBA_BIND_ALL", "")).strip().lower() in ("1", "true", "yes", "on")

def _origin_ok(origin):
    # Same-machine tool: accept any localhost / 127.0.0.1 origin at ANY port, so a Docker port
    # mapping like 8888->8788 works, plus any explicitly allowlisted origin.
    if origin in _EXTRA_ORIGINS:
        return True
    if not origin:
        # No Origin header (curl, native clients, same-origin server posts). Trusted on a local-only
        # deployment; on an exposed deployment we do NOT trust the mere absence of an Origin, the
        # auth token is required instead (enforced separately in _auth_ok).
        return not EXPOSED
    return urlparse(origin).hostname in ("localhost", "127.0.0.1")

def _auth_ok(handler):
    # When a token is configured, every /api call must present it. When none is configured, calls are
    # allowed only if the instance is not network-exposed.
    if AUTH_TOKEN:
        hdr = handler.headers.get("X-Ingest-Auth", "")
        if hdr:
            return hdr == AUTH_TOKEN
        qs = parse_qs(urlparse(handler.path).query)
        return (qs.get("token", [""])[0]) == AUTH_TOKEN
    return not EXPOSED


class H(http.server.BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json", headers=None):
        if not isinstance(body, (bytes, bytearray)):
            body = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        p = self.path.split("?")[0]
        if p in ("/", "/index.html"):
            # The static page carries no secrets; serve it so the UI can prompt for / attach the token.
            try:
                self._send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
            except Exception as e:
                self._send(500, f"cannot read index.html: {e}", "text/plain")
            return
        if p.startswith("/api/") and not _auth_ok(self):
            return self._send(401, {"error": "authentication required: set INGEST_AUTH_TOKEN and pass it via ?token= or the X-Ingest-Auth header"})
        if p == "/api/config":
            self._send(200, {"console": core.CONSOLE, "xdr": core.XDR, "hec": bool(core.HEC_URL),
                             "hecUrl": core.HEC_URL, "credsOk": core.creds_ok(),
                             "missing": core.missing_creds(),
                             "defaultAccount": (core.resolve_account() if core.creds_ok() else ""),
                             "defaultSiteId": core.DEFAULT_SITE_ID,
                             "defaultSiteName": core.DEFAULT_SITE_NAME,
                             "defaultPrefix": core.DEFAULT_PREFIX})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if not _auth_ok(self):
            return self._send(401, {"error": "authentication required: set INGEST_AUTH_TOKEN and send it via the X-Ingest-Auth header"})
        if not _origin_ok(self.headers.get("Origin")):
            return self._send(403, {"error": "cross-origin request rejected"})
        n = int(self.headers.get("Content-Length", 0) or 0)
        try:
            d = json.loads(self.rfile.read(n) or b"{}") if n else {}
        except Exception:
            d = {}
        try:
            self._route(self.path, d)
        except Exception as e:
            self._send(500, {"error": str(e)})

    def _route(self, path, d):
        core.dlog(f">>> POST {path}  params={json.dumps(d)[:1200]}")
        # If the UI sent a site NAME but not an id (client search couldn't resolve it), resolve
        # server-side so detections/flows land on the chosen site, not the account.
        if path in ("/api/rule", "/api/silent", "/api/dormant", "/api/refresh", "/api/dashboard", "/api/delete_deployment") \
                and d.get("siteName") and not d.get("siteId"):
            d["siteId"] = core.resolve_site_id(d.get("siteName"), d.get("account"))
            core.dlog(f"resolved siteName={d.get('siteName')!r} -> siteId={d.get('siteId')!r}")
        if path == "/api/connect":
            ok = core.set_creds(d)
            return self._send(200, {"credsOk": ok, "console": core.CONSOLE, "xdr": core.XDR,
                                    "hec": bool(core.HEC_URL), "account": (core.resolve_account() if ok else "")})
        if path == "/api/export":
            # Render every artifact for the current params and stream it back as a .zip download.
            # No creds required, this is pure template rendering.
            data, fname = core.export_bundle(d)
            return self._send(200, data, "application/zip",
                              {"Content-Disposition": f'attachment; filename="{fname}"'})
        if path == "/api/enumerate":
            code, res = core.lrq("dataSource.name=* | group names=array_agg_distinct(dataSource.name) | limit 1",
                                 start="24h")
            names = []
            if res.get("values"):
                try:
                    names = json.loads(res["values"][0][0])
                except Exception:
                    names = res["values"][0]
            return self._send(200, {"sources": sorted([n for n in names if n and n != "null"])})
        if path == "/api/connections":
            site = d.get("siteId") or core.DEFAULT_SITE_ID
            if site:
                conns = core.discover_connections(site_id=site)
            else:
                conns = core.discover_connections(account_id=(d.get("account") or core.resolve_account()))
            return self._send(200, {"connections": conns})
        if path == "/api/sites":
            acct = d.get("account") or core.resolve_account()
            q = (d.get("query") or "").strip()
            sp = {"limit": 200, "accountIds": acct}
            if q:
                sp["name__contains"] = q          # search-as-you-type across all sites
            code, res = core.mgmt("GET", "/web/api/v2.1/sites", params=sp)
            data = res.get("data") or {}
            rows = data.get("sites") if isinstance(data, dict) else (data if isinstance(data, list) else [])
            out = [{"id": x.get("id"), "name": x.get("name")} for x in (rows or []) if x.get("id")]
            out.sort(key=lambda s: (s.get("name") or "").lower())
            if core.DEFAULT_SITE_ID:
                out = [x for x in out if x["id"] != core.DEFAULT_SITE_ID]
                out.insert(0, {"id": core.DEFAULT_SITE_ID, "name": core.DEFAULT_SITE_NAME or "default site"})
            return self._send(200, {"sites": out, "account": acct, "defaultId": core.DEFAULT_SITE_ID,
                                    "defaultName": core.DEFAULT_SITE_NAME})
        if path == "/api/schema":
            code, res = core.sdl_schema(d.get("source", ""), start=d.get("start", "7d"))
            fields = res.get("fields", [])
            principals, actions = core.pick_fields(fields)
            return self._send(200, {"fields": fields, "principals": principals, "actions": actions,
                                    "principal": principals[0] if principals else "",
                                    "action": actions[0] if actions else ""})
        if path == "/api/exclusions":
            return self._send(200, {"tables": core.deploy_exclusions(core._normalize(d))})
        if path == "/api/inclusions":
            return self._send(200, {"tables": core.deploy_inclusions(core._normalize(d))})
        if path == "/api/delete_deployment":
            if not d.get("confirm"):
                return self._send(400, {"error": "confirmation required (confirm=true)"})
            return self._send(200, core.delete_deployment(d))
        if path == "/api/baseline_stub":
            v = core.level_view(core._normalize(d), d.get("level", "source"))
            return self._send(200, core.build_baseline_stub(v))
        if path == "/api/baseline":
            v = core.level_view(core._normalize(d), d.get("level", "source"))
            return self._send(200, core.build_baseline(v))
        if path == "/api/rule":
            v = core.level_view(core._normalize(d), d.get("level", "source"))
            return self._send(200, core.deploy_rule(v, d.get("kind")))
        if path == "/api/silent":
            v = core.level_view(core._normalize(d), d.get("level", "source"))
            return self._send(200, core.deploy_silent(v))
        if path == "/api/dashboard":
            return self._send(200, core.deploy_dashboard(core._normalize(d)))
        if path == "/api/runflow":
            return self._send(200, core.run_workflow_now(d.get("id"), d.get("versionId"), site_id=d.get("siteId"), account_id=d.get("account")))
        if path == "/api/execstatus":
            return self._send(200, core.get_execution(d.get("execId"), site_id=d.get("siteId"), account_id=d.get("account")))
        if path == "/api/refresh":
            v = core.level_view(core._normalize(d), d.get("level", "source"))
            return self._send(200, core.deploy_refresh(v))
        if path == "/api/deploy":
            return self._send(200, core.deploy_solution(d, dry_run=bool(d.get("dryRun"))))
        if path == "/api/preview_silent_rows":
            from templates import antijoin_pq
            code, res = core.lrq(antijoin_pq(d), start="24h")
            return self._send(200, res)
        return self._send(404, {"error": "unknown endpoint"})

    def log_message(self, *a):
        pass


socketserver.TCPServer.allow_reuse_address = True
if __name__ == "__main__":
    # Refuse to start network-exposed without a token: an open port here drives privileged S1 calls.
    if EXPOSED and not AUTH_TOKEN:
        sys.stderr.write(
            "REFUSING TO START: INGEST_BIND_ALL is set (network exposure) but INGEST_AUTH_TOKEN is empty.\n"
            "Set INGEST_AUTH_TOKEN=<strong-secret> and open the UI at ?token=<secret>, or unset\n"
            "INGEST_BIND_ALL and publish to the host loopback only (docker run -p 127.0.0.1:8888:8788 ...).\n")
        sys.exit(2)
    with socketserver.TCPServer((HOST, PORT), H) as httpd:
        print(f"Ingest Health deployer  ->  http://localhost:{PORT}")
        print(f"Console                 ->  {core.CONSOLE or '(S1_CONSOLE_URL not set)'}")
        print(f"SDL (XDR)               ->  {core.XDR or '(SDL_XDR_URL not set)'}")
        print(f"HEC ingest              ->  {core.HEC_URL or '(not set: the SILENT alert POST needs it)'}")
        if AUTH_TOKEN:
            print("Auth                    ->  token required (pass ?token=<secret> or X-Ingest-Auth header)")
        if EXPOSED:
            print(f"Exposure                ->  bound to {HOST} (network-reachable); token enforced on every /api call")
        elif HOST not in ("127.0.0.1", "localhost", "::1"):
            print(f"WARNING        ->  bound to {HOST} without INGEST_BIND_ALL. For network use set INGEST_BIND_ALL=1 + INGEST_AUTH_TOKEN, or publish to 127.0.0.1 only.")
        if not core.creds_ok():
            print(f"WARNING        ->  missing credentials: {', '.join(core.missing_creds())}")
        print("Ctrl-C to stop.")
        httpd.serve_forever()
