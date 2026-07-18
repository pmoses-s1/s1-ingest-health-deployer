#!/usr/bin/env python3
"""
s1-ingest-health-deployer core: the SentinelOne client, the deploy functions, and a headless
deploy_solution(). Baselines expected ingest VOLUME per source (or device) and deploys four
health detections (SILENT / DROP / SPIKE / NEW). Used by:

  - app/server.py   the interactive one-click UI (Docker)

Credentials are read from environment variables first (the documented path for the
repo and for CI), then, as a local convenience, from the Claude Desktop config if it
is present. Nothing is hard-coded and no secret is ever committed.

Environment variables (see .env.example):
  S1_CONSOLE_URL            https://<tenant>.sentinelone.net           (required)
  S1_CONSOLE_API_TOKEN      console JWT / ApiToken                     (required)
  SDL_XDR_URL               https://xdr.<region>.sentinelone.net       (dashboards/schema)
  SDL_CONFIG_READ_KEY       SDL config-read key                        (dashboard get)
  SDL_CONFIG_WRITE_KEY      SDL config-write key                       (dashboard put)
  S1_HEC_INGEST_URL         UAM/HEC ingest base                        (SILENT/DORMANT alerts)
                            (HEC ingest authenticates with S1_CONSOLE_API_TOKEN)
  S1_DEFAULT_SITE_ID        default deploy site id                     (optional)
  S1_DEFAULT_SITE_NAME      default deploy site name                   (optional)
  S1_ACCOUNT_ID             default account id                         (optional)
  UEBA_PREFIX               default naming prefix (default "UEBA")     (optional)
"""
import json, os, time, re, urllib.request, urllib.error, pathlib, datetime

from templates import (savelookup_pq, stub_baseline_pq, stub_pq, rule_body, watchdog_workflow, refresh_workflow,
                       notifier_workflow, deployed_flow_names,
                       dashboard_json, slug, antijoin_pq, entity_field, level_view)
import dashboard as _dash   # multi-tab review dashboard

# ---------------------------------------------------------------- credentials
def _load_env():
    """Env vars first; fall back to the local Claude Desktop config if present."""
    cfg = {}
    cfg_path = os.path.expanduser("~/Library/Application Support/Claude/claude_desktop_config.json")
    if os.path.exists(cfg_path):
        try:
            cfg = json.load(open(cfg_path))["mcpServers"]["sentinelone-mcp"]["env"]
        except Exception:
            cfg = {}
    def g(*keys, default=""):
        for k in keys:
            if os.environ.get(k):
                return os.environ[k]
        for k in keys:
            if cfg.get(k):
                return cfg[k]
        return default
    return g

_g = _load_env()
CONSOLE     = (_g("S1_CONSOLE_URL", "S1_BASE_URL", "SDL_CONSOLE_URL")).rstrip("/")
XDR         = (_g("SDL_XDR_URL", "SDL_BASE_URL")).rstrip("/")
JWT         = _g("S1_CONSOLE_API_TOKEN", "S1_API_TOKEN", "SDL_CONSOLE_API_TOKEN")
K_LOG_READ  = _g("SDL_LOG_READ_KEY") or JWT
# Each SDL key falls back to the console token, never to another SDL key: a config-write key does
# not grant read (and vice versa), just as a config key does not grant log read.
K_CFG_READ  = _g("SDL_CONFIG_READ_KEY") or JWT
K_CFG_WRITE = _g("SDL_CONFIG_WRITE_KEY") or JWT
HEC_URL     = (_g("S1_HEC_INGEST_URL", "S1_UAM_ALERT_INTERFACE_URL")).rstrip("/")
HEC_TOKEN   = JWT   # HEC ingest uses the console token; no separate S1_HEC_INGEST_TOKEN

DEFAULT_SITE_ID    = _g("S1_SITE", "S1_DEFAULT_SITE_ID")
DEFAULT_SITE_NAME  = _g("S1_DEFAULT_SITE_NAME") or (DEFAULT_SITE_ID and "default site") or ""
DEFAULT_ACCOUNT_ID = _g("S1_ACCOUNT", "S1_ACCOUNT_ID", "S1_DEFAULT_ACCOUNT_ID")
DEFAULT_PREFIX     = _g("INGEST_PREFIX", "UEBA_PREFIX") or "INGEST"
# Built-in Hyperautomation SDL/HTTP integration (action-pack) id. A NEW connection is created UNDER
# this integration; the server returns the connection's own id, but HA flows bind the INTEGRATION id.
# Tenant-validated; override per tenant if the built-in id differs.
SDL_INTEGRATION_ID = _g("INGEST_SDL_INTEGRATION_ID", "UEBA_SDL_INTEGRATION_ID", "S1_SDL_INTEGRATION_ID") or "ea6018b7-2a2f-44ca-b9b6-27a0434b0503"
# High-volume, always-on feeds excluded from the watch-all baseline by default (the deployer's own EDR
# telemetry and Windows event logs dwarf every other feed and are not useful ingest-health signals).
# Applied only when the caller does not manage exclusions itself. Override via INGEST_DEFAULT_EXCLUSIONS
# (comma-separated; set to an empty string to disable the default).
_dex = _g("INGEST_DEFAULT_EXCLUSIONS") or "SentinelOne,Windows Event Logs"
DEFAULT_SOURCE_EXCLUSIONS = [] if _dex.strip().lower() in ("none", "off", "-") else [s.strip() for s in _dex.split(",") if s.strip()]

def creds_ok():
    return bool(CONSOLE and JWT)

def missing_creds():
    m = []
    if not CONSOLE: m.append("S1_CONSOLE_URL")
    if not JWT:     m.append("S1_CONSOLE_API_TOKEN")
    return m

def set_creds(d):
    """Update credentials at runtime from the in-UI Connect form. Only the console URL + token are
    required; SDL and HEC values are optional and fall back to the console token. Held in memory
    only, never written to disk."""
    global CONSOLE, JWT, XDR, K_LOG_READ, K_CFG_READ, K_CFG_WRITE, HEC_URL, HEC_TOKEN, _RESOLVED_ACCT
    CONSOLE   = (d.get("console") or CONSOLE or "").rstrip("/")
    JWT       = d.get("token") or JWT
    XDR       = (d.get("xdr") or XDR or "").rstrip("/")
    # Each SDL key falls back to the console token, never to another SDL key (a write key does not
    # grant read, and a config key does not grant log read).
    K_LOG_READ  = d.get("logKey") or JWT
    K_CFG_READ  = d.get("cfgReadKey") or JWT
    K_CFG_WRITE = d.get("cfgWriteKey") or JWT
    HEC_URL     = (d.get("hecUrl") or HEC_URL or "").rstrip("/")
    HEC_TOKEN   = JWT   # HEC ingest uses the console token
    _RESOLVED_ACCT = None      # re-resolve the account for the new tenant
    return creds_ok()

_RESOLVED_ACCT = None
def resolve_account():
    """The account id to default to: env value, else the tenant's first active account."""
    global _RESOLVED_ACCT
    if DEFAULT_ACCOUNT_ID:
        return DEFAULT_ACCOUNT_ID
    if _RESOLVED_ACCT is not None:
        return _RESOLVED_ACCT
    _RESOLVED_ACCT = ""
    code, res = mgmt("GET", "/web/api/v2.1/accounts", params={"states": "active", "limit": 1})
    rows = (res or {}).get("data") or []
    if rows and rows[0].get("id"):
        _RESOLVED_ACCT = rows[0]["id"]
    return _RESOLVED_ACCT

def resolve_site_id(name, account_id=None):
    """Resolve a site name to its id. name__contains breaks on spaces, so search on the first
    token then exact-match the full name client-side. Returns '' if not found."""
    name = (name or "").strip()
    if not name:
        return ""
    account_id = account_id or resolve_account()
    code, res = mgmt("GET", "/web/api/v2.1/sites",
                     params={"limit": 100, "accountIds": account_id, "name__contains": name.split()[0]})
    data = res.get("data") or {}
    rows = data.get("sites") if isinstance(data, dict) else (data if isinstance(data, list) else [])
    for s in (rows or []):
        if (s.get("name") or "").strip().lower() == name.lower():
            return s.get("id") or ""
    return ""

def rule_exists(name, site_id=None, account_id=None):
    """Return the id of a scheduled detection with this exact name on the scope, else None."""
    params = {"isLegacy": "false", "limit": 50, "name__contains": name}
    if site_id: params["siteIds"] = site_id
    elif account_id: params["accountIds"] = account_id
    code, res = mgmt("GET", "/web/api/v2.1/cloud-detection/rules", params=params)
    for r in (res.get("data") or []):
        if (r.get("name") or "") == name:
            return r.get("id")
    return None

def workflow_exists(name, site_id=None, account_id=None):
    """Return the id of a Hyperautomation workflow with this exact name on the scope, else None."""
    params = {"limit": 200}
    if site_id: params["siteIds"] = site_id
    elif account_id: params["accountIds"] = account_id
    code, res = mgmt("GET", "/web/api/v2.1/hyper-automate/api/v1/workflows", params=params)
    for w in (res.get("data") or res.get("workflows") or []):
        wf = w.get("workflow") or w
        if (wf.get("name") or "") == name:
            return w.get("id") or wf.get("id")
    return None

def _scope_qs(site_id=None, account_id=None):
    """Deploy scope query string: a site if given, else the account."""
    if site_id:
        return f"siteIds={site_id}"
    if account_id:
        return f"accountIds={account_id}"
    return ""

# ---------------------------------------------------------------- logging
# Off by default: set INGEST_DEBUG=1 to enable. When on, secrets are redacted before write and the
# file is capped so a token or response body never sits on disk in cleartext or grows unbounded.
DEBUG_ON = os.environ.get("INGEST_DEBUG", os.environ.get("UEBA_DEBUG", "")).strip().lower() in ("1", "true", "yes", "on")
DEBUG_LOG = pathlib.Path(os.environ.get("INGEST_LOG", os.environ.get("UEBA_LOG", "ingest_debug.log")))
_DEBUG_MAX_BYTES = 5 * 1024 * 1024
_REDACT_RX = [
    # Authorization: Bearer <...>  /  "authorization": "..."
    (re.compile(r'(?i)(authorization["\']?\s*[:=]\s*["\']?)(bearer\s+)?[A-Za-z0-9._\-]+'), r'\1\2<redacted>'),
    # any sensitive key = value  (token / apiToken / password / secret / *Key / *_key)
    (re.compile(r'(?i)(["\']?(?:api[_-]?token|token|password|secret|[A-Za-z0-9]*key)["\']?\s*[:=]\s*["\']?)[^"\'\s,}]+'), r'\1<redacted>'),
    # bare JWTs anywhere
    (re.compile(r'eyJ[A-Za-z0-9._\-]{10,}'), '<redacted-jwt>'),
]
def _redact(s):
    s = str(s)
    for rx, repl in _REDACT_RX:
        s = rx.sub(repl, s)
    return s
def dlog(msg):
    if not DEBUG_ON:
        return
    try:
        if DEBUG_LOG.exists() and DEBUG_LOG.stat().st_size > _DEBUG_MAX_BYTES:
            DEBUG_LOG.write_text("")   # simple rotation: truncate once over the cap
        with open(DEBUG_LOG, "a") as f:
            f.write(f"{datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}  {_redact(msg)}\n")
    except Exception:
        pass

# ---------------------------------------------------------------- HTTP helpers
def _req(url, method, headers, body=None, timeout=120):
    data = None
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
    r = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            hdrs = {k.lower(): v for k, v in resp.getheaders()}
            dlog(f"HTTP {method} {url} -> {resp.status} ({len(raw)}B)")
            return resp.status, raw, hdrs
    except urllib.error.HTTPError as e:
        eb = e.read()
        dlog(f"HTTP {method} {url} -> {e.code}  body={eb[:800].decode('utf-8','replace')}")
        return e.code, eb, {k.lower(): v for k, v in (e.headers.items() if e.headers else [])}
    except Exception as e:
        dlog(f"HTTP {method} {url} -> EXCEPTION {type(e).__name__}: {e}")
        return 502, json.dumps({"error": str(e)}).encode(), {}

def mgmt(method, path, body=None, params=None):
    url = CONSOLE + path
    if params:
        sep = "&" if "?" in url else "?"
        url += sep + "&".join(f"{k}={v}" for k, v in params.items())
    h = {"Authorization": "ApiToken " + JWT, "Content-Type": "application/json", "Accept": "application/json"}
    code, raw, _ = _req(url, method, h, body)
    try:
        return code, json.loads(raw or b"{}")
    except Exception:
        return code, {"raw": raw.decode("utf-8", "replace")}

def sdl_cfg(ep, body, key):
    code, raw, _ = _req(XDR + ep, "POST",
                        {"Authorization": "Bearer " + key, "Content-Type": "application/json"}, body)
    try:
        return code, json.loads(raw or b"{}")
    except Exception:
        return code, {"raw": raw.decode("utf-8", "replace")}

def sdl_schema(source, start="7d", sample=5):
    body = {"queryType": "log", "maxCount": sample, "filter": f"dataSource.name='{source}'", "startTime": start}
    code, raw, _ = _req(XDR + "/api/query", "POST",
                        {"Authorization": "Bearer " + K_LOG_READ, "Content-Type": "application/json"}, body)
    try:
        j = json.loads(raw or b"{}")
    except Exception:
        return code, {"error": raw.decode("utf-8", "replace")}
    fields = set()
    for m in (j.get("matches") or []):
        attrs = m.get("attributes") or m.get("values") or {}
        if isinstance(attrs, dict):
            fields.update(attrs.keys())
    return code, {"fields": sorted(fields)}

def lrq(query, start="24h", end=None, poll_secs=115):
    launch = {"queryType": "PQ", "startTime": start, "queryPriority": "HIGH",
              "pq": {"query": query, "resultType": "TABLE"}, "tenant": True}
    if end:
        launch["endTime"] = end
    h = {"Authorization": "Bearer " + JWT, "Content-Type": "application/json", "Accept": "application/json"}
    code, raw, hdrs = _req(CONSOLE + "/sdl/v2/api/queries", "POST", h, launch, timeout=60)
    if code >= 400:
        return code, {"error": raw.decode("utf-8", "replace")}
    j = json.loads(raw or b"{}")
    qid = j.get("id")
    tag = hdrs.get("x-dataset-query-forward-tag")
    if not qid:
        return 502, {"error": "no query id", "body": j}
    ph = {"Authorization": "Bearer " + JWT, "Accept": "application/json"}
    if tag:
        ph["X-Dataset-Query-Forward-Tag"] = tag
    deadline = time.time() + poll_secs
    last = {}
    while time.time() < deadline:
        pc, praw, _ = _req(f"{CONSOLE}/sdl/v2/api/queries/{qid}?lastStepSeen=0", "GET", ph, timeout=60)
        try:
            last = json.loads(praw or b"{}")
        except Exception:
            last = {}
        done = last.get("stepsCompleted"); total = last.get("totalSteps")
        if total is not None and done is not None and done >= total:
            d = last.get("data") or {}
            cols = [c.get("name") for c in (d.get("columns") or [])]
            return 200, {"columns": cols, "values": d.get("values") or [], "matchCount": d.get("matchCount")}
        time.sleep(3)
    return 202, {"columns": [], "values": [], "pending": True, "queryId": qid}

def ha_import(workflow_json, site_id=None, account_id=None):
    url = f"{CONSOLE}/web/api/v2.1/hyper-automate/api/public/workflow-import-export/import?{_scope_qs(site_id, account_id)}"
    h = {"Authorization": "ApiToken " + JWT, "Content-Type": "application/json", "Accept": "application/json"}
    if isinstance(workflow_json, (str, bytes)):
        workflow_json = json.loads(workflow_json)
    code, raw, _ = _req(url, "POST", h, {"data": workflow_json}, timeout=90)
    try:
        return code, json.loads(raw or b"{}")
    except Exception:
        return code, {"raw": raw.decode("utf-8", "replace")}

def ha_publish(wf_id, site_id=None, account_id=None):
    url = f"{CONSOLE}/web/api/v2.1/hyper-automate/api/v1/workflows/{wf_id}/publish?{_scope_qs(site_id, account_id)}"
    h = {"Authorization": "ApiToken " + JWT, "Content-Type": "application/json", "Accept": "application/json"}
    code, raw, _ = _req(url, "POST", h, {}, timeout=60)
    return code, (raw.decode("utf-8", "replace") if raw else "")

def ha_activate(wf_id, version_id, site_id=None, account_id=None):
    url = (f"{CONSOLE}/web/api/v2.1/hyper-automate/api/public/workflows/{wf_id}/{version_id}"
           f"/activation?{_scope_qs(site_id, account_id)}")
    h = {"Authorization": "ApiToken " + JWT, "Content-Type": "application/json", "Accept": "application/json"}
    code, raw, _ = _req(url, "POST", h, {"data": {"timeout": 86400}}, timeout=60)
    return code

def run_workflow_now(wf_id, version_id, site_id=None, account_id=None):
    """Trigger an ACTIVE workflow (including a scheduled-trigger flow) to run immediately via the
    manual-execution endpoint. Despite the name 'manual', this runs an active scheduled flow now.
    The flow must be active. Returns {ok, execId, httpcode, detail}."""
    site = site_id or DEFAULT_SITE_ID or None
    acct = account_id or resolve_account() or None
    url = f"{CONSOLE}/web/api/v2.1/hyper-automate/api/public/workflow-execution/manual/{wf_id}/{version_id}?{_scope_qs(site, acct)}"
    h = {"Authorization": "ApiToken " + JWT, "Content-Type": "application/json", "Accept": "application/json"}
    code, raw, _ = _req(url, "POST", h, {"data": {}}, timeout=60)
    try: res = json.loads(raw or b"{}")
    except Exception: res = {"raw": raw.decode("utf-8", "replace")}
    exec_id = (res.get("data") or {}).get("id") or res.get("id")
    dlog(f"run_workflow_now wf={wf_id} ver={version_id} httpcode={code} exec={exec_id}")
    return {"step": "refresh_run_now", "ok": code < 300, "execId": exec_id, "httpcode": code, "detail": res}

def get_execution(exec_id, site_id=None, account_id=None):
    """Poll a workflow execution. Returns {ok, state, executed_actions, error_actions, httpcode}.
    state goes Running -> Completed (or Failed)."""
    site = site_id or DEFAULT_SITE_ID or None
    acct = account_id or resolve_account() or None
    url = f"{CONSOLE}/web/api/v2.1/hyper-automate/api/public/workflow-execution/{exec_id}?{_scope_qs(site, acct)}"
    h = {"Authorization": "ApiToken " + JWT, "Accept": "application/json"}
    code, raw, _ = _req(url, "GET", h, timeout=60)
    try: res = json.loads(raw or b"{}")
    except Exception: res = {}
    d = res.get("data") or res
    return {"ok": code < 300, "state": d.get("state"), "executed_actions": d.get("executed_actions"),
            "error_actions": d.get("error_actions"), "httpcode": code}

def discover_connections(site_id=None, account_id=None):
    params = {"limit": 200}
    if site_id:
        params["siteIds"] = site_id
    elif account_id:
        params["accountIds"] = account_id
    code, res = mgmt("GET", "/web/api/v2.1/hyper-automate/api/v1/workflows", params=params)
    rows = (res.get("data") or res.get("workflows") or []) if isinstance(res, dict) else []
    seen = {}
    for w in rows:
        wf = w.get("workflow") or w
        if (wf.get("state") or "").lower() != "active":
            continue
        name = wf.get("name", "")
        iids = list(w.get("integrationIds") or w.get("integration_ids") or [])
        for act in (w.get("actions") or []):
            iid = act.get("integration_id") if isinstance(act, dict) else None
            if iid:
                iids.append(iid)
        for iid in iids:
            seen.setdefault(iid, [])
            if name and name not in seen[iid]:
                seen[iid].append(name)
    def score(uses):
        j = " ".join(uses).lower()
        pinned = any((u or "").strip().startswith("0") for u in uses)
        return (pinned, ("sdl" in j or "rba" in j or "ingest health" in j or "risk collector" in j), len(uses))
    return [{"id": iid, "label": iid[:8] + "… used by: " + ", ".join(uses[:2])}
            for iid, uses in sorted(seen.items(), key=lambda kv: score(kv[1]), reverse=True)]

def create_sdl_connection(name=None, site_id=None, account_id=None):
    """Create a Hyperautomation 'SentinelOne SDL' (Bearer) connection from the configured console
    creds so the HA flows bind + activate with no manual UI step. User-initiated (a button in the
    config flow). The console token is sent once over TLS and stored ENCRYPTED server-side (never
    persisted by the deployer). Returns {ok, id, connectionId, name, httpcode}: `id` is the
    INTEGRATION id that flows bind to (binding the connection id fails at runtime), `connectionId`
    is the credential instance created under it."""
    site = site_id or DEFAULT_SITE_ID or None
    acct = account_id or resolve_account() or None
    if not creds_ok():
        return {"ok": False, "error": "missing credentials: " + ", ".join(missing_creds())}
    host = (CONSOLE or "").replace("https://", "").replace("http://", "").rstrip("/")
    nm = re.sub(r"[\"'\\\r\n\t]", "", str(name or "")).strip()
    nm = re.sub(r"\s+", " ", nm)[:80] or "S1 SDL (ingest-health deployer)"
    body = {"data": {
        "integration_id": SDL_INTEGRATION_ID, "name": nm, "url": host, "protocol": "https://",
        "port": 443, "tunnel": False, "tunnel_id": None, "is_default": False,
        "authentication_type": "api_key",
        "authentication_data": {"authentication_type": "api_key", "api_key": JWT,
                                "way_to_pass": "header", "way_to_pass_input": "Authorization",
                                "way_to_pass_prefix": "Bearer"},
        "is_pna": False},
        "filter": {"siteIds": ([site] if site else []), "accountIds": ([] if site else ([acct] if acct else []))}}
    code, res = mgmt("POST", f"/web/api/v2.1/hyper-automate/api/v1/connections?{_scope_qs(site, acct)}", body=body)
    cid = res.get("id") if isinstance(res, dict) else None
    dlog(f"create_sdl_connection name={nm!r} site={site} httpcode={code} conn={cid}")
    if code < 300 and cid:
        return {"ok": True, "id": SDL_INTEGRATION_ID, "connectionId": cid, "name": nm, "httpcode": code}
    return {"ok": False, "id": None, "name": nm, "httpcode": code,
            "detail": (res if not isinstance(res, dict) else {k: v for k, v in res.items() if k != "authentication_data"})}

# ---------------------------------------------------------------- field picker
_USER_RX = re.compile(r"(?:^|[._])user(?:\.|$)|username|principal|actor\.name|\.email(?:\b|$)", re.I)
_HOST_RX = re.compile(r"hostname|\.host\.name|\.device\.name|endpoint\.name|\.computer(?:\b|$)"
                      r"|^agent\.(?:uuid|id|name)|^device\.(?:id|name)|^endpoint\.(?:id|name)", re.I)
_IP_RX   = re.compile(r"\.ip\.address|\.ip_addr|(?:^|[._])ip(?:\b|$)|ipv4|ipv6", re.I)
_TS_RX   = re.compile(r"(?:^|[._])(?:time|timestamp|date|ts)(?:\b|$|_)", re.I)
_ACTION_EXACT = ("activity_name", "action", "event.type", "event.action", "event.category",
                 "event.outcome", "outcome", "result", "status", "disposition", "verdict",
                 "class_name", "category_name", "rule.name", "threat.classification")

def pick_fields(fields):
    principals, actions = [], []
    for f in fields:
        if _TS_RX.search(f):
            continue
        if f in _ACTION_EXACT or re.search(r"activity|event\.type|action|outcome|status|verdict|category_name", f, re.I):
            actions.append(f)
        if _USER_RX.search(f) or _HOST_RX.search(f) or _IP_RX.search(f):
            principals.append(f)
    def rank(cands, pref):
        return sorted(cands, key=lambda c: (0 if c in pref else 1, len(c)))
    principals = rank(principals, ("actor.user.email_addr", "actor.user.name", "src_endpoint.ip",
                                   "device.name", "user.name", "user.email_addr"))
    actions = rank(actions, _ACTION_EXACT)
    return principals, actions

# ---------------------------------------------------------------- deploy steps
def _hec():
    # Security: the SILENT watchdog's alert POST authenticates at RUNTIME via the bound HA
    # connection (SentinelOne SDL / HEC), never a literal header. We therefore embed only the
    # {{HEC_TOKEN}} placeholder in the flow body, never the live console token, so no secret is
    # ever persisted in the workflow JSON stored on the tenant (readable by anyone with flow
    # access). Deploying without a bound connection imports the flow inactive; bind + activate in
    # the HA UI. The URL is a public regional host and is safe to embed.
    return (HEC_URL or "{{HEC_URL}}", "{{HEC_TOKEN}}")

def build_baseline_stub(p):
    """Create the ingest-volume baseline as a fast schema-only stub (see templates.stub_baseline_pq).
    The real baseline is built by the refresh flow's run-now. Aborts the deploy only if the stub fails."""
    q = stub_pq(p, "core")
    code, res = lrq(q, start="30d", poll_secs=60)
    ok = code < 300
    table = p["baselineTable"]
    dlog(f"build_baseline_stub table={table} code={code} ok={ok}")
    return {"step": "baseline", "table": table, "ok": ok, "stub": True, "kind": "core", "detail": res}

def build_baseline(p):
    q = savelookup_pq(p)
    table = p["baselineTable"]
    code, res = lrq(q, start=f"{int(p['baselineDays'])*24}h", poll_secs=300)
    dlog(f"build_baseline table={table} code={code} pending={res.get('pending', False)}")
    return {"step": "baseline", "kind": "core", "table": table, "query": q,
            "ok": code in (200, 202), "pending": res.get("pending", False),
            "rows": (res.get("values") or [{}])[0] if res.get("values") else None, "detail": res}

def deploy_rule(p, kind):
    site = p.get("siteId") or DEFAULT_SITE_ID
    p["siteId"] = site   # rule_body scopes on this: siteIds if set, else accountIds
    body = rule_body(p, kind)
    name = body["data"]["name"]
    existing = rule_exists(name, site or None, None if site else (p.get("account") or resolve_account()))
    if existing:
        dlog(f"deploy_rule[{kind}] SKIP (exists) id={existing}")
        return {"step": f"rule_{kind}", "name": name, "id": existing, "created": False,
                "enabled": None, "skipped": True, "reason": "a detection with this name already exists"}
    code, res = mgmt("POST", "/web/api/v2.1/cloud-detection/rules", body)
    rid = (res.get("data") or {}).get("id")
    enabled = False
    if rid:
        flt = {"ids": [rid]}
        if site:
            flt["siteIds"] = [site]
        else:
            flt["accountIds"] = [p.get("account") or resolve_account()]
        ec, _ = mgmt("PUT", "/web/api/v2.1/cloud-detection/rules/enable", {"filter": flt})
        enabled = ec < 300
    dlog(f"deploy_rule[{kind}] created={bool(rid)} id={rid} enabled={enabled} httpcode={code}")
    return {"step": f"rule_{kind}", "name": body["data"]["name"], "id": rid,
            "created": bool(rid), "enabled": enabled, "detail": res}

def _deploy_flow(p, wf, step):
    site = p.get("siteId") or DEFAULT_SITE_ID or None
    acct = p.get("account") or resolve_account() or None
    existing = workflow_exists(wf["name"], site, acct)
    if existing:
        dlog(f"{step} SKIP (exists) id={existing}")
        return {"step": step, "name": wf["name"], "id": existing, "imported": False,
                "skipped": True, "reason": "a workflow with this name already exists"}
    code, res = ha_import(json.dumps(wf), site_id=site, account_id=acct)
    wid = res.get("id"); ver = res.get("version_id")
    activated = published = False
    if wid:
        if p.get("sdlIntegrationId"):
            activated = ha_activate(wid, ver, site_id=site, account_id=acct) < 300
        if not activated:
            pc, _ = ha_publish(wid, site_id=site, account_id=acct); published = isinstance(pc, int) and pc < 300
    dlog(f"{step} imported={bool(wid)} id={wid} activated={activated} published={published} httpcode={code}")
    return {"step": step, "name": wf["name"], "id": wid, "version_id": ver,
            "imported": bool(wid), "activated": activated, "published": published, "detail": res}

def deploy_silent(p):
    return _deploy_flow(p, watchdog_workflow(p, *_hec()), f"silent_watchdog_{p.get('scope','source')}")

def deploy_dashboard(p):
    path = f"/dashboards/{p['prefix']} {p['source']} Ingest Health"
    _, cur = sdl_cfg("/api/getFile", {"path": path}, K_CFG_READ)
    if isinstance(cur, dict) and cur.get("version") is not None:
        dlog(f"deploy_dashboard SKIP (exists) path={path!r}")
        return {"step": "dashboard", "path": path, "ok": False, "skipped": True,
                "reason": "a dashboard already exists at this path"}
    code, res = sdl_cfg("/api/putFile", {"path": path, "content": _dash.review_dashboard_json(p)}, K_CFG_WRITE)
    dlog(f"deploy_dashboard path={path!r} ok={code < 300} httpcode={code}")
    return {"step": "dashboard", "path": path, "ok": code < 300, "detail": res}

def deploy_refresh(p):
    return _deploy_flow(p, refresh_workflow(p), f"refresh_flow_{p.get('scope','source')}")

def deploy_notifier(p):
    """Deploy the daily 'run last' health-notifier flow (emails ops support if any owned flow's
    latest run is not Completed). Only deployed when an Operations Support Email is provided."""
    if not (p.get("notifyEmail") or "").strip():
        return {"step": "notifier_flow", "skipped": True, "reason": "no Operations Support Email set", "name": "notifier"}
    return _deploy_flow(p, notifier_workflow(p), "notifier_flow")

def build_manifest(p):
    """Self-contained record of every component this deployment creates (detection rules, per-level
    refresh + SILENT watchdog + notifier flows, dashboard, datatables, bound SDL connection, scope).
    Doubles as the delete/update spec: re-upload to delete exactly this deployment. No secrets."""
    p = _normalize(p)
    src = p["source"]; dets = p["types"]; levels = p["levels"]
    rules, tables = [], []
    for level in levels:
        v = level_view(p, level)
        for k in ("drop", "spike", "new"):
            if k in dets:
                rules.append({"name": rule_body(v, k)["data"]["name"], "kind": k, "level": level})
        tables.append(v["baselineTable"])
    flows = []
    for level in levels:
        v = level_view(p, level)
        flows.append({"name": refresh_workflow(v)["name"], "role": "baseline-refresh", "level": level,
                      "schedule": f"{refresh_workflow(v)['hour']:02d}:{refresh_workflow(v)['minute']:02d} {refresh_workflow(v)['tz']}"})
        if "silent" in dets:
            flows.append({"name": watchdog_workflow(v, *_hec())["name"], "role": "watchdog", "level": level})
    if p.get("notifyEmail"):
        flows.append({"name": notifier_workflow(p)["name"], "role": "notifier", "schedule": "daily (run last)", "notifyEmail": p["notifyEmail"]})
    if p.get("exclusionsEnabled"): tables.append(p["entityExclTable"])
    if p.get("inclusionsEnabled") and p.get("inclEntities"): tables.append(p["entityInclTable"])
    return {
        "manifest_version": "1", "tool": "s1-ingest-health-deployer",
        "generated_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scope": {"account": p.get("account"), "siteId": p.get("siteId"), "siteName": p.get("siteName")},
        "prefix": p["prefix"], "source": src, "levels": levels, "detections": dets,
        "connectionId": p.get("sdlIntegrationId") or None,
        "schedule": {"tz": p["scheduleTz"], "staggerMin": p["refreshStaggerMin"]},
        "notifyEmail": p.get("notifyEmail") or None,
        "components": {"detectionRules": rules, "flows": flows,
                       "dashboard": f"/dashboards/{p['prefix']} {src} Ingest Health", "tables": tables},
        "params": {"prefix": p["prefix"], "source": src, "siteId": p.get("siteId"),
                   "siteName": p.get("siteName"), "account": p.get("account"), "levels": levels,
                   "types": dets, "deviceField": p.get("deviceField"), "sources": p.get("sources"),
                   "exclusionsEnabled": bool(p.get("exclusionsEnabled")),
                   "inclusionsEnabled": bool(p.get("inclusionsEnabled")), "notifyEmail": p.get("notifyEmail")},
    }

def delete_from_manifest(manifest):
    """Delete exactly the deployment described by an uploaded manifest. Recommended update path:
    delete-by-manifest, then reconfigure + redeploy."""
    if not isinstance(manifest, dict):
        return {"error": "invalid manifest"}
    params = manifest.get("params") if isinstance(manifest.get("params"), dict) else manifest
    if not (params.get("prefix") and params.get("source")):
        return {"error": "manifest missing prefix/source; cannot scope deletion"}
    res = delete_deployment(params)
    res["from_manifest"] = True
    return res


# --------------------------------------------------------------- export / save artifacts
def _fsafe(s):
    """Filesystem-safe fragment for zip entry names."""
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(s)).strip("_") or "artifact"


def _extract_flow_pqs(wf):
    """Pull every embedded PowerQuery out of an HA workflow's http_request payloads.
    Returns [(action_name, pq_string), ...]. LRQ launch/poll payloads are JSON with a
    pq.query field; the gzip alert/indicator payloads are not JSON and are skipped."""
    out = []
    for a in (wf.get("actions") or []):
        d = ((a.get("action") or {}).get("data") or {})
        if d.get("action_type") != "http_request":
            continue
        payload = d.get("payload")
        if not payload or not isinstance(payload, str):
            continue
        try:
            j = json.loads(payload)
        except Exception:
            continue
        q = (j.get("pq") or {}).get("query") if isinstance(j, dict) else None
        if q:
            out.append((d.get("name") or "query", q))
    return out


def export_bundle(p):
    """Render EVERY artifact the deployer would create, for the current parameters, and return
    (zip_bytes, filename). No API calls, works offline / pre-deploy. Includes: the baseline
    savelookup queries, the scheduled detection rule bodies (PQ + full STAR rule JSON), the
    SILENT/DORMANT and nightly-refresh HA flows (full JSON) PLUS the PowerQueries extracted from
    inside those flows, the review dashboard JSON, and any exclusion lookup CSVs. Secrets are not
    embedded: the flow HEC token is left as the {{HEC_TOKEN}} placeholder."""
    import io, zipfile
    p = _normalize(p)
    dets = p.get("types") or ["silent", "drop", "spike", "new"]
    pfx = slug(p["prefix"]) or "INGEST"
    src_slug = slug(p["source"]) or "sources"
    files = {}

    # deployment manifest (also the delete/update spec: re-upload to remove exactly this deployment)
    files["manifest.json"] = json.dumps(build_manifest(p), indent=2)

    hec = (HEC_URL or "{{HEC_URL}}", "{{HEC_TOKEN}}")   # never export the real token

    def add_flow(wf, folder, base):
        files[f"{folder}/{base}.workflow.json"] = json.dumps(wf, indent=2)
        for qname, q in _extract_flow_pqs(wf):
            files[f"{folder}/queries/{base}__{_fsafe(qname)}.pq"] = q

    # per-level: baseline savelookup PQ, scheduled detection rules, SILENT watchdog, refresh flow
    for level in p["levels"]:
        v = level_view(p, level)
        files[f"baselines/{level}__{v['baselineTable']}.pq"] = savelookup_pq(v)
        for k in ("drop", "spike", "new"):
            if k in dets:
                rb = rule_body(v, k)
                base = _fsafe(rb["data"]["name"])
                files[f"detections/{base}.rule.json"] = json.dumps(rb, indent=2)
                files[f"detections/{base}.pq"] = rb["data"]["scheduledParams"]["query"]
        if "silent" in dets:
            add_flow(watchdog_workflow(v, *hec), "watchdogs", f"SILENT_{level}")
        add_flow(refresh_workflow(v), "flows", f"baseline-refresh_{level}")
    # daily 'run last' health-notifier (exported when an Operations Support Email is set)
    if (p.get("notifyEmail") or "").strip():
        add_flow(notifier_workflow(p), "flows", "health-notifier")

    # one review dashboard spanning every enabled level
    files[f"dashboard/{pfx}_{src_slug}_IngestHealth.dashboard.json"] = _dash.review_dashboard_json(p)

    # exclusion / inclusion lookup CSVs
    if p.get("exclusionsEnabled"):
        files[f"exclusions/{p['entityExclTable']}"] = _excl_csv(p.get("exclEntities", []))
    if p.get("inclusionsEnabled") and p.get("inclEntities"):
        files[f"inclusions/{p['entityInclTable']}"] = _incl_csv(p["inclEntities"])

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for path, text in sorted(files.items()):
            z.writestr(path, text if isinstance(text, str) else str(text))
    return buf.getvalue(), f"{pfx}_{src_slug}_ingest_health_artifacts.zip"


def _as_list(v):
    """Accept a list, or a comma/newline-separated string, return a clean list of values."""
    if not v:
        return []
    if isinstance(v, list):
        items = v
    else:
        items = str(v).replace(",", "\n").splitlines()
    return [str(x).strip() for x in items if str(x).strip()]

def _excl_csv(values):
    """CSV body for an exclusion lookup table (header always present; rows optional)."""
    import datetime
    today = datetime.date.today().isoformat()
    rows = "".join(f"{v},excluded via UEBA deployer,,{today}\n" for v in values)
    return "value,reason,owner,added\n" + rows

def deploy_exclusion_table(path, values, step):
    """Create a CSV exclusion lookup table. SKIP if it already exists so a user's manual
    edits (or a previously-seeded list) are never overwritten on re-deploy."""
    _, cur = sdl_cfg("/api/getFile", {"path": path}, K_CFG_READ)
    if isinstance(cur, dict) and cur.get("version") is not None:
        dlog(f"deploy_exclusion SKIP (exists) path={path!r}")
        return {"step": step, "path": path, "ok": False, "skipped": True,
                "reason": "exclusion list already exists; left unchanged to preserve manual edits"}
    code, res = sdl_cfg("/api/putFile", {"path": path, "content": _excl_csv(values)}, K_CFG_WRITE)
    dlog(f"deploy_exclusion path={path!r} ok={code < 300} httpcode={code} rows={len(values)}")
    return {"step": step, "path": path, "ok": code < 300, "detail": res, "rows": len(values)}

def deploy_exclusions(p):
    """Deploy the entity exclusion (denylist) lookup table (header-only if no seed values supplied)."""
    return [
        deploy_exclusion_table(f"/datatables/{p['entityExclTable']}", p.get("exclEntities", []), "exclusion_entities"),
    ]


def _incl_csv(values):
    """CSV body for an inclusion (allowlist) lookup table."""
    import datetime
    today = datetime.date.today().isoformat()
    rows = "".join(f"{v},included via UEBA deployer,,{today}\n" for v in values)
    return "value,reason,owner,added\n" + rows

def deploy_inclusion_table(path, values, step):
    """Create a CSV inclusion (allowlist) lookup table. SKIP if it already exists so manual edits
    are preserved on re-deploy."""
    _, cur = sdl_cfg("/api/getFile", {"path": path}, K_CFG_READ)
    if isinstance(cur, dict) and cur.get("version") is not None:
        dlog(f"deploy_inclusion SKIP (exists) path={path!r}")
        return {"step": step, "path": path, "ok": False, "skipped": True,
                "reason": "inclusion list already exists; left unchanged to preserve manual edits"}
    code, res = sdl_cfg("/api/putFile", {"path": path, "content": _incl_csv(values)}, K_CFG_WRITE)
    dlog(f"deploy_inclusion path={path!r} ok={code < 300} httpcode={code} rows={len(values)}")
    return {"step": step, "path": path, "ok": code < 300, "detail": res, "rows": len(values)}

def deploy_inclusions(p):
    """Deploy the entity inclusion (allowlist) lookup table. Written only when it has rows, an
    empty allowlist would drop all data, so an empty allowlist is treated as off."""
    out = []
    if p.get("inclEntities"):
        out.append(deploy_inclusion_table(f"/datatables/{p['entityInclTable']}", p["inclEntities"], "inclusion_entities"))
    return out

def delete_deployment(p):
    """Delete ONLY the artifacts this deployer created for a given prefix + source, a safe fallback
    if a deploy was a mistake. Scoping safeguards:
      - detection rules and SILENT/DORMANT flows match the exact name prefix '<prefix> - <source>';
      - the refresh flow matches the exact name '<prefix> <source> Baseline Refresh';
      - the dashboard is the exact path '/dashboards/<prefix> <source> Anomalies';
      - datatables are the exact baseline/advanced/exclusion names derived from the prefix + source.
    Nothing outside that naming scope is touched. HA flows are deactivated BEFORE deletion (an active
    flow cannot be archived). Requires prefix + source."""
    p = _normalize(p)
    site = p.get("siteId") or DEFAULT_SITE_ID or None
    acct = p.get("account") or resolve_account() or None
    prefix, src = p["prefix"], p["source"]
    if not prefix or not src:
        return {"error": "prefix and source are required to scope the deletion"}
    rule_flow_prefix = f"{prefix} - {src}"                 # detection rules + SILENT watchdog flow
    refresh_prefix = f"{prefix} {src} "                    # per-level: "<prefix> <src> <LVL> Ingest Baseline Refresh"
    notifier_name = f"{prefix} {src} Ingest Health Notifier"
    def _is_refresh(name):
        return name.startswith(refresh_prefix) and name.endswith("Ingest Baseline Refresh")
    skey = "siteIds" if site else "accountIds"
    sval = site if site else acct
    scope = {skey: sval}
    out = {"prefix": prefix, "source": src, "site": site, "rules": [], "flows": [], "dashboard": None, "tables": []}

    # 1) detection rules (scheduled STAR). Enumerate by NAME query: the siteIds list filter is
    # unreliable on some tenants (returns 0 even when rules exist), the name `query` param is not.
    _, res = mgmt("GET", "/web/api/v2.1/cloud-detection/rules",
                  params={"isLegacy": False, "limit": 200, "query": prefix})
    rules = (res.get("data") or []) if isinstance(res, dict) else []
    match = [(r.get("id"), r.get("name")) for r in rules
             if isinstance(r, dict) and str(r.get("name", "")).startswith(rule_flow_prefix) and r.get("id")]
    if match:
        ids = [rid for rid, _ in match]
        dc, _dr = mgmt("DELETE", "/web/api/v2.1/cloud-detection/rules", {"filter": {"ids": ids, "accountIds": [acct]}})
        for rid, rname in match:
            out["rules"].append({"id": rid, "name": rname, "deleted": dc < 300})

    # 2) HA flows (SILENT/DORMANT + refresh): deactivate THEN delete
    _, res = mgmt("GET", "/web/api/v2.1/hyper-automate/api/v1/workflows", params={"limit": 200, "name__contains": prefix})
    rows = (res.get("data") or []) if isinstance(res, dict) else []
    for w in rows:
        wf = w.get("workflow") or w
        name = wf.get("name", ""); wid = wf.get("id") or w.get("id")
        if not wid:
            continue
        if name.startswith(rule_flow_prefix) or _is_refresh(name) or name == notifier_name:
            mgmt("POST", f"/web/api/v2.1/hyper-automate/api/public/workflows/{wid}/deactivate", params=scope)
            dc, _ = mgmt("DELETE", f"/web/api/v2.1/hyper-automate/api/v1/workflows/{wid}", params=scope)
            out["flows"].append({"id": wid, "name": name, "deleted": dc < 300})

    # 3) dashboard config file
    dpath = f"/dashboards/{prefix} {src} Ingest Health"
    _, cur = sdl_cfg("/api/getFile", {"path": dpath}, K_CFG_READ)
    if isinstance(cur, dict) and cur.get("version") is not None:
        dc, _ = sdl_cfg("/api/putFile", {"path": dpath, "expectedVersion": cur["version"], "deleteFile": True}, K_CFG_WRITE)
        out["dashboard"] = {"path": dpath, "deleted": dc < 300}

    # 4) datatables: both per-level ingest-volume baselines + exclusion + inclusion lookups
    tables = [p["baselineTableSource"], p["baselineTableDevice"], p["entityExclTable"], p["entityInclTable"]]
    for t in tables:
        tp = f"/datatables/{t}"
        _, cur = sdl_cfg("/api/getFile", {"path": tp}, K_CFG_READ)
        if isinstance(cur, dict) and cur.get("version") is not None:
            dc, _ = sdl_cfg("/api/putFile", {"path": tp, "expectedVersion": cur["version"], "deleteFile": True}, K_CFG_WRITE)
            out["tables"].append({"table": t, "deleted": dc < 300})

    out["summary"] = {"rules": len(out["rules"]), "flows": len(out["flows"]),
                      "dashboard": bool(out["dashboard"]), "tables": len(out["tables"])}
    dlog(f"delete_deployment prefix={prefix!r} src={src!r} -> {out['summary']}")
    return out

# ---------------------------------------------------------------- orchestration
def _normalize(p):
    p = dict(p)
    # ingest-health always deploys the four health detections unless the UI narrows the set
    if not p.get("types"):
        p["types"] = ["silent", "drop", "spike", "new"]
    if not p.get("method"):
        p["method"] = "robust"
    # Source level is ALWAYS deployed; device level is the optional add-on. Device is requested via
    # levels containing 'device', scope=='device', or deviceEnabled=true. Source is always present and
    # first (so its refresh anchors local midnight and device staggers after it).
    _lv = p.get("levels") or []
    _want_device = ("device" in _lv) or (p.get("scope") == "device") or bool(p.get("deviceEnabled"))
    p["levels"] = ["source"] + (["device"] if _want_device else [])
    # Foolproof free-text query inputs so a stray character can never break a deployed query.
    # Each source goes into  dataSource.name in ('A','B')  (single-quoted), so drop quotes /
    # backslashes / semicolons / pipes / newlines; the device field is an identifier that may be a
    # quoted ref, so keep double quotes but strip the definite breakers. All length-capped.
    def _safe_src(s):
        return re.sub(r"[\"'\\;|\r\n]", "", str(s or "")).strip()[:120]
    def _safe_field(s):
        return re.sub(r"['\\;|\r\n]", "", str(s or "")).strip()[:120]
    p["sources"] = [x for x in (_safe_src(s) for s in _as_list(p.get("sources"))) if x]
    # device level needs a device field; source level always groups by dataSource.name
    p["deviceField"] = _safe_field(p.get("deviceField")) or "src.endpoint.name"
    # `scope`/`entity` reflect the FIRST enabled level for any single-level caller; the per-level
    # templates.level_view() re-pins scope+entity+table for each level at deploy time.
    p["scope"] = "device" if p["levels"][0] == "device" else "source"
    p["entity"] = p["deviceField"] if p["scope"] == "device" else "dataSource.name"
    # Foolproof the prefix: keep ONLY letters, digits and underscore, so a stray space or symbol
    # can never break a deploy (it appears in rule/flow names and the dashboard path). Never empty.
    _rawpfx = str(p.get("prefix") or DEFAULT_PREFIX or "INGEST")
    p["prefix"] = re.sub(r"[^A-Za-z0-9_]+", "_", _rawpfx).strip("_") or "INGEST"
    # 'source' here is a naming LABEL for the monitored scope (a single source name, or e.g.
    # 'AllSources' / 'Fiv-Sources'); it stamps the artifact names.
    if not p.get("source"):
        srcs = p["sources"]
        p["source"] = (slug(srcs[0]) if len(srcs) == 1 else (f"{len(srcs)}Sources" if srcs else "AllSources"))
    _pfx = slug(p["prefix"])
    # per-level tables (each level is its own datatable); baselineTable defaults to the source one
    # for any generic single-table caller.
    p["baselineTableSource"] = _pfx + slug(p["source"]) + "SourceIngestBaseline"
    p["baselineTableDevice"] = _pfx + slug(p["source"]) + "DeviceIngestBaseline"
    p["baselineTable"] = p["baselineTableDevice"] if p["scope"] == "device" else p["baselineTableSource"]
    p["baselineDays"] = int(p.get("baselineDays") or 30)
    p["baselineHours"] = int(p.get("baselineHours") or p["baselineDays"] * 24)
    p["baselineGranularity"] = "hourly" if p.get("baselineGranularity") == "hourly" else "daily"
    # Per-level refresh scheduling: anchor at 00:00 in the user's timezone, stagger N min apart so
    # the `| nolimit` savelookups never overlap (one nolimit query per account at a time).
    p["scheduleTz"] = (p.get("scheduleTz") or _g("INGEST_SCHEDULE_TZ", "UEBA_SCHEDULE_TZ") or "UTC").strip() or "UTC"
    try:
        p["refreshStaggerMin"] = max(5, int(p.get("refreshStaggerMin") or _g("INGEST_REFRESH_STAGGER_MIN") or 30))
    except (TypeError, ValueError):
        p["refreshStaggerMin"] = 30
    # Operations Support Email: recipient for the daily 'run last' health-notifier flow.
    p["notifyEmail"] = (p.get("notifyEmail") or _g("INGEST_NOTIFY_EMAIL", "UEBA_NOTIFY_EMAIL") or "").strip()
    p["account"] = p.get("account") or DEFAULT_ACCOUNT_ID
    p["siteId"] = p.get("siteId") or DEFAULT_SITE_ID
    p.setdefault("topK", 1000)
    p.setdefault("zHard", 3.0)
    p.setdefault("silentFloor", 5)
    p.setdefault("silentZ", 2.5)
    p.setdefault("severities", {"silent": "High", "drop": "High", "spike": "Medium", "new": "Low"})
    # ---- exclusions (optional; ignore known-good entities, e.g. a decommissioned feed) ----
    # If the caller does not manage exclusions (no exclEntities / exclusionsEnabled key), seed the
    # default high-volume exclusions so a watch-all deploy never baselines the two firehose feeds.
    _excl_managed = ("exclEntities" in p) or ("exclusionsEnabled" in p)
    p["exclusionsEnabled"] = bool(p.get("exclusionsEnabled"))
    p["entityExclTable"] = _pfx + "entityExclusions.csv"
    p["exclEntities"] = _as_list(p.get("exclEntities"))
    if not _excl_managed and DEFAULT_SOURCE_EXCLUSIONS:
        p["exclusionsEnabled"] = True
        p["exclEntities"] = list(DEFAULT_SOURCE_EXCLUSIONS)
    # ---- inclusions (optional; ALLOWLIST, monitor ONLY the listed entities) ----
    p["inclEntities"] = _as_list(p.get("inclEntities"))
    # only ON when non-empty: an empty allowlist would drop all data, so treat enabled-but-empty as OFF.
    p["inclusionsEnabled"] = bool(p.get("inclusionsEnabled")) and bool(p["inclEntities"])
    p["entityInclTable"] = _pfx + "entityInclusions.csv"
    return p

def deploy_solution(p, dry_run=False, log=None):
    """Run the full deploy for the resolved detection set. Used by the UI deploy flow.
    dry_run renders every artifact via the templates without calling any API."""
    log = log or (lambda *a, **k: None)
    p = _normalize(p)
    dets = p["types"]
    log(f"source={p['source']} site={p['siteId'] or '(default)'} method={p['method']} detections={dets}")

    if dry_run:
        steps = []
        if p.get("exclusionsEnabled"):
            steps.append(("exclusion_entities", f"/datatables/{p['entityExclTable']}"))
        if p.get("inclusionsEnabled"):
            steps.append(("inclusion_entities", f"/datatables/{p['entityInclTable']}"))
        for level in p["levels"]:
            v = level_view(p, level)
            steps.append((f"{level}_baseline (stub; real build via refresh run-now)", stub_baseline_pq(v)))
            for k in ("drop", "spike", "new"):
                if k in dets:
                    steps.append((f"{level}_rule_{k}", rule_body(v, k)["data"]["scheduledParams"]["query"]))
            if "silent" in dets:
                steps.append((f"{level}_silent_watchdog", watchdog_workflow(v, *_hec())["name"]))
            steps.append((f"{level}_refresh_flow", refresh_workflow(v)["name"]))
        steps.append(("dashboard", f"/dashboards/{p['prefix']} {p['source']} Ingest Health"))
        if (p.get("notifyEmail") or "").strip():
            steps.append(("notifier_flow", notifier_workflow(p)["name"]))
        for name, detail in steps:
            log(f"[dry-run] {name}: {str(detail)[:180]}")
        return {"ok": True, "dry_run": True, "detections": dets, "levels": p["levels"], "method": p["method"],
                "steps": [{"step": n} for n, _ in steps]}

    if not creds_ok():
        return {"ok": False, "error": "missing credentials: " + ", ".join(missing_creds())}

    results = []; skipped = []
    def run(r):
        results.append(r)
        if r.get("skipped"):
            skipped.append(r.get("step"))
        ok = r.get("ok") or r.get("created") or r.get("imported")
        state = "SKIPPED (exists)" if r.get("skipped") else ("ok" if ok else "FAILED")
        log(f"{r.get('step')}: {state} {r.get('id') or r.get('table') or r.get('path') or ''}")
        return r

    # Exclusion lookup tables must exist before any baseline/detection query references them.
    if p.get("exclusionsEnabled"):
        for r in deploy_exclusions(p):
            rr = run(r)
            if not (rr.get("ok") or rr.get("skipped")):
                err = f"exclusion table {rr.get('path')} could not be created; aborting deploy"
                log(err)
                return {"ok": False, "error": err, "steps": results}

    # Inclusion (allowlist) lookup tables must also exist before any query references them.
    if p.get("inclusionsEnabled"):
        for r in deploy_inclusions(p):
            rr = run(r)
            if not (rr.get("ok") or rr.get("skipped")):
                err = f"inclusion table {rr.get('path')} could not be created; aborting deploy"
                log(err)
                return {"ok": False, "error": err, "steps": results}

    # For EACH enabled level (source and/or device): stub its own baseline table, deploy its own
    # scheduled detections + SILENT watchdog + refresh flow, and trigger that level's baseline build.
    for level in p["levels"]:
        v = level_view(p, level)
        base = run(build_baseline_stub(v))
        if not base.get("ok"):
            err = f"{level} baseline stub could not be created for {base.get('table')}; aborting deploy"
            log(err)
            return {"ok": False, "error": err, "steps": results}
        for k in ("drop", "spike", "new"):
            if k in dets:
                run(deploy_rule(v, k))
        if "silent" in dets:
            run(deploy_silent(v))
        rf = run(deploy_refresh(v))
        if p.get("skipRunNow"):
            continue   # test/deferred mode: import + activate only, don't trigger the baseline build now
        if rf.get("imported") and rf.get("activated") and rf.get("id") and rf.get("version_id"):
            rn = run_workflow_now(rf["id"], rf["version_id"], site_id=p.get("siteId"), account_id=p.get("account"))
            rn["step"] = f"{level}_refresh_run_now"
            rn["note"] = f"{level} baseline build triggered; verify completion in its HA Activity"
            run(rn)
    # one dashboard spanning every enabled level
    run(deploy_dashboard(p))
    # daily 'run last' health-notifier: emails ops support if any owned flow's latest run failed
    run(deploy_notifier(p))

    ok = all(r.get("ok") or r.get("created") or r.get("imported") or r.get("skipped") for r in results)
    notice = ("Some artifacts already existed and were skipped. Deploy with a different naming prefix, "
              "or delete the existing artifacts, then retry.") if skipped else None
    return {"ok": ok, "detections": dets, "method": p["method"],
            "skipped": skipped, "notice": notice, "steps": results,
            "manifest": build_manifest(p)}
