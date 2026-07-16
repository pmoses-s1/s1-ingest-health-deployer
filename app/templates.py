"""
Ingest-health artifact templates - every query / rule / workflow / dashboard the deployer
renders, as pure functions of the UI parameter dict `p`.

Model: a single ENTITY dimension (a data source, or a device within one or more sources) and a
single metric (event VOLUME = count of events). The deployer baselines the expected per-entity
volume over a trailing window and ships four detections:

  SILENT  - an established feed produces ZERO events now (feed dark).       HA watchdog (anti-join LRQ -> OCSF alert)
  DROP    - volume far BELOW baseline but not zero (feed degraded).         scheduled detection
  SPIKE   - volume far ABOVE baseline (flood / loop / misconfig).           scheduled detection
  NEW     - an entity ingesting now with no baseline (unexpected feed).     scheduled detection

The scheduled-detection engine runs on a pre-aggregated data layer with no left join / dataset,
so SILENT (which needs the baseline datatable joined to live volume) runs as a Hyperautomation
watchdog, exactly as in the UEBA deployer this tool is a sibling of.

`p` keys (all set by the UI / core._normalize):
  prefix, source (naming label for the monitored scope), scope ('source'|'device'),
  sources (list of dataSource.name to monitor; empty = all), entity (grouping field),
  deviceField (device scope only), baselineTable, baselineDays, baselineHours, topK,
  zHard, silentZ, silentFloor, method, baselineGranularity, renotify, runInterval,
  noiseFilter, severities{silent,drop,spike,new}, exclusions/inclusions (entity allow/deny lists)
"""
import re


def slug(s):
    return re.sub(r"[^A-Za-z0-9]", "", s or "")


def _nf(p):
    nf = (p.get("noiseFilter") or "").strip()
    return (" " + nf) if nf else ""


def _bucket(p):
    """Baseline time bucket. 'hourly' baselines per hour (detection compares the last 1h); the
    default 'daily' baselines per day (compares the last 24h)."""
    return "1h" if (p.get("baselineGranularity") == "hourly") else "1d"


def _win_minutes(p):
    """Detection live-window minutes, matched to the baseline bucket (60 hourly, 1440 daily)."""
    return 60 if (p.get("baselineGranularity") == "hourly") else 1440


def entity_field(p):
    """The field each row is grouped by: the data source name (source scope) or a device field."""
    if p.get("entity"):
        return p["entity"]
    if p.get("scope") == "device":
        return p.get("deviceField") or "dataSource.name"
    return "dataSource.name"


# A deployment can enable BOTH a source-level and a device-level view in one solution. Each view is
# a shallow copy of p pinned to one scope (its own entity field + its own baseline table). Every
# query/rule/flow function below operates on a single view, so the two levels reuse identical logic.
def level_view(p, level):
    v = dict(p)
    v["scope"] = "device" if level == "device" else "source"
    if v["scope"] == "device":
        v["entity"] = p.get("deviceField") or "src.endpoint.name"
        v["deviceField"] = v["entity"]
    else:
        v["entity"] = "dataSource.name"
    v["baselineTable"] = p.get("baselineTableDevice") if v["scope"] == "device" else p.get("baselineTableSource")
    # fall back to a single derived table if the per-level names were not set (direct callers/tests)
    if not v["baselineTable"]:
        v["baselineTable"] = p.get("baselineTable") or (slug(p.get("prefix","INGEST")) + slug(p.get("source","")) +
                                                        ("Device" if v["scope"]=="device" else "Source") + "IngestBaseline")
    return v


def _level_word(p):
    return "device" if p.get("scope") == "device" else "source"


def _sources_predicate(p):
    """Restrict the scan to the monitored sources. Empty list = all sources."""
    srcs = [s for s in (p.get("sources") or []) if s]
    if not srcs:
        return "dataSource.name = *"
    if len(srcs) == 1:
        return f"dataSource.name = '{srcs[0]}'"
    inlist = ", ".join(f"'{s}'" for s in srcs)
    return f"dataSource.name in ({inlist})"


def _base(p):
    """Leading filter for every query: monitored sources + optional noise filter."""
    return _sources_predicate(p) + _nf(p)


# --------------------------------------------------------------- exclusions / inclusions (entity scoping)
def _excl(p, field=None):
    """Denylist anti-join. Drops rows whose entity is in the exclusion CSV lookup table."""
    if not p.get("exclusionsEnabled") or not field:
        return ""
    t = p.get("entityExclTable")
    return f"| lookup _ex = reason from {t} by value =:anycase {field} | filter _ex = null " if t else ""


def _incl(p, field=None):
    """Allowlist semi-join. Keeps ONLY rows whose entity is in the inclusion CSV lookup table.
    Applied only when the allowlist is non-empty (an empty allowlist must never drop all data)."""
    if not p.get("inclusionsEnabled") or not field or not p.get("inclEntities"):
        return ""
    t = p.get("entityInclTable")
    return f"| lookup _in = reason from {t} by value =:anycase {field} | filter _in = * " if t else ""


def _scope(p, field=None):
    return _excl(p, field) + _incl(p, field)


# --------------------------------------------------------------- baseline PQ (per-entity volume)
def savelookup_pq(p):
    """Per-entity per-bucket event volume over the baseline window, reduced to mean/stddev/median/
    p95/p05 and persisted as a datatable the detections look up."""
    ent = entity_field(p)
    return (
        f"{_base(p)} "
        f"| nolimit "  # raise scan cap so the full baseline window completes (LRQ only)
        f"| filter {ent} = * " + _scope(p, ent) + f""
        f"| group bucket_count = count() by bucket = timebucket('{_bucket(p)}'), entity_v = {ent} "
        f"| group baseline_avg = avg(bucket_count), baseline_stddev = stddev(bucket_count), "
        f"baseline_med = median(bucket_count), baseline_p95 = p95(bucket_count), baseline_p05 = pct(5, bucket_count), "
        f"n_buckets = count() by entity_v "
        f"| filter n_buckets >= 2 "
        f"| sort -baseline_avg | limit {int(p.get('topK', 1000))} "
        f"| savelookup '{p['baselineTable']}'"
    )


# Sentinel stub so the detection lookups resolve immediately; the real baseline is built by the
# refresh flow's run-now (and nightly). The '__stub__' key never matches a real entity.
_STUB_KEYS = ["entity_v"]
_STUB_NUMS = ["baseline_avg", "baseline_stddev", "baseline_med", "baseline_p95", "baseline_p05", "n_buckets"]

def stub_pq(p, kind="core"):
    lets = "".join(f"| let {n} = number(0) " for n in _STUB_NUMS)
    cols = ", ".join(_STUB_KEYS + _STUB_NUMS)
    return (f"dataSource.name = * | limit 1 | group _n = count() by entity_v = '__stub__' "
            f"{lets}| columns {cols} | limit 1 | savelookup '{p['baselineTable']}'")

def stub_baseline_pq(p):
    return stub_pq(p, "core")


# --------------------------------------------------------------- SILENT anti-join (LRQ, watchdog + dashboard)
def antijoin_pq(p):
    """Established feeds (baseline_avg >= floor) with ZERO events in the live window = feed dark."""
    ent = entity_field(p)
    floor = p.get("silentFloor", 5)
    return (
        f"| left join "
        f"a = ( | dataset 'config://datatables/{p['baselineTable']}' | columns entity_v, baseline_avg, baseline_stddev, baseline_med ), "
        f"b = ( {_base(p)} {ent}=* | group live_count=count() by entity_v={ent} ) "
        f"on a.entity_v = b.entity_v " + _scope(p, "entity_v") + f""
        f"| let lc = number(live_count) | let avg = number(baseline_avg) "
        f"| filter avg >= {floor} | filter lc == 0 "
        f"| let direction = 'SILENT' | sort -avg "
        f"| columns entity_v, baseline_avg, baseline_med, baseline_stddev, direction | limit 500"
    )


# --------------------------------------------------------------- scheduled rule bodies (SPIKE / DROP / NEW)
def _rule_pq(p, kind):
    ent = entity_field(p)
    z = p.get("zHard", 3.0)
    method = p.get("method", "robust")
    base = (f"{_base(p)} | filter {ent} = * " + _scope(p, ent) + f""
            f"| group live_count = count() by entity_v = {ent} ")
    if kind == "new":
        return (base +
                f"| lookup baseline_avg = baseline_avg from {p['baselineTable']} by entity_v = entity_v "
                f"| filter !(baseline_avg = *) | sort -live_count "
                f"| columns entity_v, live_count | limit 500")
    lk = (f"| lookup baseline_avg = baseline_avg, baseline_stddev = baseline_stddev, "
          f"baseline_p95 = baseline_p95, baseline_p05 = baseline_p05, n_buckets = n_buckets "
          f"from {p['baselineTable']} by entity_v = entity_v "
          f"| filter baseline_avg = * | let sd = number(baseline_stddev) | let z = (live_count - baseline_avg) / sd ")
    if kind == "spike":
        cond = "live_count > baseline_p95" if method == "robust" else f"z >= {z}"
        return (base + lk + f"| filter {cond} | let direction = 'SPIKE' | sort -z "
                f"| columns entity_v, live_count, baseline_avg, baseline_p95, z, direction | limit 500")
    if kind == "drop":
        # DROP = degraded but not dark; SILENT handles the zero case.
        cond = "live_count < baseline_p05" if method == "robust" else f"z <= -{z}"
        return (base + lk + f"| filter live_count > 0 | filter {cond} | let direction = 'DROP' | sort z "
                f"| columns entity_v, live_count, baseline_avg, baseline_p05, z, direction | limit 500")
    raise ValueError(kind)


def rule_body(p, kind):
    src = p.get("source") or "sources"
    z = p.get("zHard", 3.0)
    sev = p.get("severities", {})
    _hourly = p.get("baselineGranularity") == "hourly"
    _win = "1h" if _hourly else "24h"
    _lookback = 60 if _hourly else 1440
    ent = entity_field(p)
    scope_word = "device" if p.get("scope") == "device" else "source"
    lvl = scope_word.upper()
    names = {
        "spike": (f"{p['prefix']} - {src} {lvl} ingest SPIKE (volume flood)", sev.get("spike", "Medium"),
                  f"Ingest-health SPIKE. Fires when a {scope_word}'s {_win} event volume is far ABOVE its "
                  f"baseline (p95 / z>={z}) in {p['baselineTable']} (grouped by {ent}). Possible loop, "
                  f"misconfig, or flood."),
        "drop":  (f"{p['prefix']} - {src} {lvl} ingest DROP (feed degraded)", sev.get("drop", "High"),
                  f"Ingest-health DROP. Fires when a {scope_word}'s {_win} event volume is far BELOW its "
                  f"baseline (p05 / z<=-{z}) in {p['baselineTable']} but not zero (SILENT covers zero)."),
        "new":   (f"{p['prefix']} - {src} {lvl} ingest NEW feed (no baseline)", sev.get("new", "Low"),
                  f"Ingest-health NEW. Fires when a {scope_word} is ingesting in {_win} but has NO entry in "
                  f"the baseline {p['baselineTable']} (unexpected or first-seen feed)."),
    }
    name, severity, desc = names[kind]
    return {
        "data": {
            "name": name, "description": desc,
            "queryType": "scheduled", "queryLang": "2.0",
            "severity": severity, "status": "Disabled", "expirationMode": "Permanent",
            "treatAsThreat": "UNDEFINED", "networkQuarantine": False,
            "coolOffSettings": {"renotifyMinutes": int(p.get("renotify", 1440))},
            "entityMappings": [{"columnName": "entity_v"}],
            "scheduledParams": {
                "query": _rule_pq(p, kind),
                "runIntervalMinutes": int(p.get("runInterval", 60)), "lookbackWindowMinutes": _lookback,
                "alertPerRow": True, "disableStreaksLogic": False,
                "threshold": {"value": 0, "operator": "Greater"},
            },
        },
        "filter": ({"siteIds": [p["siteId"]]} if p.get("siteId") else {"accountIds": [p["account"]]}),
    }


# --------------------------------------------------------------- connection binding for HA flows
def _bind_connection(actions, sdl_integration_id, hec_integration_id=None):
    for a in actions:
        act = a.get("action", {})
        d = act.get("data", {})
        if d.get("action_type") != "http_request":
            continue
        is_alert = "/v1/alerts" in (d.get("url") or "")
        if is_alert:
            intg = hec_integration_id or sdl_integration_id
            if intg:
                act["integration_id"] = intg
                d["use_authentication_data"] = True
                d.get("headers", {}).pop("Authorization", None)
        else:
            if not sdl_integration_id:
                continue
            act["integration_id"] = sdl_integration_id
            d["use_authentication_data"] = True


# --------------------------------------------------------------- SILENT watchdog HA flow
def watchdog_workflow(p, hec_url, hec_token, kind="silent"):
    """SILENT watchdog: daily anti-join LRQ (baseline datatable LEFT JOIN live volume) that flags
    established feeds with zero live events and posts one OCSF S1 Security Alert per run. Same HA
    scaffold as the UEBA deployer's SILENT watchdog; the entity here is a data source or device."""
    src = p.get("source") or "sources"
    lvl = _level_word(p).upper()
    ajq = antijoin_pq(p)
    sev_id = {"Low": 2, "Medium": 3, "High": 4, "Critical": 5}.get(p.get("severities", {}).get("silent", "High"), 4)
    hec_scope = p.get("hecScope") or (f"{p['account']}:{p['siteId']}" if p.get("siteId") else p["account"])
    lrq_payload = (
        '{\n  "queryType": "PQ",\n  "tenant": true,\n'
        '  "startTime": "{{Function.DELTA_NOW(24)}}",\n  "endTime": "{{Function.DATETIME_NOW()}}",\n'
        '  "queryPriority": "HIGH",\n  "pq": {\n    "query": ' + _json_str(ajq) + ',\n    "resultType": "TABLE"\n  }\n}')
    import json as _json
    _MS = "{{Function.DATETIME_TO_MS(Function.DATETIME_NOW())}}"
    _desc = (f"Ingest-health silent feeds (baseline avg>={p.get('silentFloor',5)}/bucket, zero live) vs "
             f"{p['baselineTable']}: {{{{local_var.silent_summary}}}}")
    _alert = {
        "finding_info": {
            "uid": "{{local_var.alert_uid}}",
            "title": f"{p['prefix']} - {src} {lvl} ingest health SILENT (feed dark)",
            "desc": _desc,
            "related_events": [{
                "message": "SILENT feed(s): {{local_var.rowcount}}. Top {{local_var.top_entity}}",
                "time": "@@MS@@", "uid": "{{local_var.ind_uid}}", "severity_id": sev_id,
                "class_uid": 1001, "type_uid": 100101, "category_uid": 1, "activity_id": 1,
                "observables": [
                    {"name": "device.hostname", "type_id": 1, "type": "string", "typeName": "Hostname", "value": "{{local_var.top_entity}}"},
                ],
            }],
        },
        "resources": [{"uid": "{{local_var.device_uid}}", "name": "{{local_var.top_entity}}", "type_id": 1, "type": "host"}],
        "category_uid": 2, "category_name": "Findings",
        "class_uid": 99602001, "class_name": "S1 Security Alert",
        "type_uid": 9960200101, "type_name": "S1 Security Alert: Create",
        "activity_id": 1,
        "metadata": {
            "version": "1.6.0-dev",
            "extension": {"name": "s1", "uid": "998", "version": "0.1.0"},
            "product": {"name": "Hyperautomation", "vendor_name": "SentinelOne"},
            "logged_time": "@@MS@@", "modified_time": "@@MS@@",
        },
        "time": "@@MS@@", "attack_surface_ids": [1],
        "severity_id": sev_id, "state_id": 1, "s1_classification_id": 1,
    }
    alert_payload = _json.dumps(_alert, separators=(",", ":")).replace('"@@MS@@"', _MS)
    _indicator = {
        "message": "Ingest health SILENT: {{local_var.rowcount}} feed(s). Top {{local_var.top_entity}}",
        "time": "@@MS@@",
        "device": {"uid": "{{local_var.device_uid}}", "name": "{{local_var.top_entity}}",
                   "type_id": 1, "hostname": "{{local_var.top_entity}}"},
        "metadata": {
            "version": "1.6.0-dev", "product": {"name": "Hyperautomation", "vendor_name": "SentinelOne"},
            "extensions": [{"name": "s1", "uid": "998", "version": "0.1.0"}],
            "profiles": ["s1/security_indicator"], "uid": "{{local_var.ind_uid}}",
        },
        "type_uid": 100101, "activity_id": 1, "class_uid": 1001, "category_uid": 1,
        "observables": [
            {"name": "device.hostname", "type_id": 1, "value": "{{local_var.top_entity}}"},
        ],
        "severity_id": sev_id, "attack_surface_id": 1,
    }
    indicator_payload = _json.dumps(_indicator, separators=(",", ":")).replace('"@@MS@@"', _MS)

    def http(name, method, url, payload, headers, tag_desc, use_auth=True):
        return {"name": name, "action_type": "http_request", "public_action_id": None, "method": method,
                "url": url, "url_path": None, "url_prefix": None, "payload": payload, "parameters": [],
                "retry_on_status_codes": [500], "ssl_verification": True, "timeout": 90,
                "headers": headers, "use_authentication_data": use_auth, "use_proxy": False,
                "redirect_follow": True, "continue_on_fail": True, "body_type": "json"}

    A = lambda t, tag, data, eid, conn, parent=None, desc="": {
        "action": {"type": t, "tag": tag, "connection_id": None, "connection_name": "" if t == "http_request" else None,
                   "use_connection_name": False, "integration_id": None, "data": data, "state": "active",
                   "description": desc, "client_data": {"position": {"x": 0, "y": 0},
                   "dimensions": {"width": 256, "height": 100}, "collapsed": False},
                   "snippet_workflow_id": None, "snippet_version_id": None},
        "export_id": eid, "connected_to": conn, "parent_action": parent}

    actions = [
        A("scheduled_trigger", "core_action",
          {"name": "Scheduled Trigger", "action_type": "scheduled_trigger", "schedule_method": "interval",
           "until": None, "max_runs": 0,
           "schedule_value": [{"schedule_method": "interval", "interval_unit": "minutes",
                               "interval_value": int(p.get("watchdogIntervalMin", 60))}],
           "start_at": None, "start_at_method": "immediately", "ends_on": "never"},
          8, [{"target": 4, "custom_handle": None}], None,
          f"Run every {int(p.get('watchdogIntervalMin', 60))} min."),
        A("http_request", "integration",
          http("Find Silent Feeds", "post", "{{Connection.protocol}}{{Connection.url}}/sdl/v2/api/queries",
               lrq_payload, {"Content-Type": "application/json", "Accept": "application/json"}, "", True),
          4, [{"target": 9, "custom_handle": None}], None, "Launch async anti-join LRQ (SDL connection)."),
        A("variable", "core_action",
          {"name": "Set LRQ Refs", "action_type": "variable", "variables": [
              {"name": "query_id", "value": "{{find-silent-feeds.body.id}}", "should_use_as_output": False, "is_secret": False},
              {"name": "forward_tag", "value": "{{Function.JQ(find-silent-feeds.headers, \"to_entries | map(select(.key|ascii_downcase==\\\"x-dataset-query-forward-tag\\\")) | .[0].value\", true)}}", "should_use_as_output": False, "is_secret": False}],
           "variables_scope": "local"},
          9, [{"target": 6, "custom_handle": None}], None, "Capture LRQ id + forward tag."),
        A("loop", "core_action",
          {"name": "Poll Until Done", "action_type": "loop", "loop_type": "while", "number_of_iterations": "60",
           "object_to_iterate": "", "is_parallel": False},
          6, [{"target": 5, "custom_handle": "inner"}], None, "Poll until done."),
        A("http_request", "integration",
          http("Poll Silent Feeds", "get", "{{Connection.protocol}}{{Connection.url}}/sdl/v2/api/queries/{{local_var.query_id}}?lastStepSeen=0",
               None, {"Accept": "application/json", "X-Dataset-Query-Forward-Tag": "{{local_var.forward_tag}}"}, "", True),
          5, [{"target": 2, "custom_handle": None}], 6, "Poll LRQ."),
        A("condition", "core_action",
          {"name": "Query Done", "action_type": "condition", "condition_type": "multi", "condition": None,
           "conditions": [{"input_value": "{{poll-silent-feeds.body.stepsCompleted}}", "compared_value": "{{poll-silent-feeds.body.totalSteps}}", "comparison_operator": "equals"}],
           "conditions_relationship": "and"},
          2, [{"target": 10, "custom_handle": "true"}, {"target": 7, "custom_handle": "false"}], 6, "Done?"),
        A("delay", "core_action", {"name": "Retry Delay", "action_type": "delay", "time_unit": "seconds", "value": 5},
          7, [], 6, "Wait, then re-poll."),
        A("variable", "core_action",
          {"name": "Set Row Count", "action_type": "variable", "variables": [
              {"name": "rowcount", "value": "{{Function.JQ(poll-silent-feeds.body.data.values, \"length\", true)}}", "should_use_as_output": False, "is_secret": False},
              {"name": "top_entity", "value": "{{Function.JQ(poll-silent-feeds.body.data.values, \"(.[0][0] // \\\"-\\\")\", true)}}", "should_use_as_output": False, "is_secret": False},
              {"name": "silent_summary", "value": "{{Function.JQ(poll-silent-feeds.body.data.values, \"map(.[0]) | join(\\\", \\\")\", true)}}", "should_use_as_output": False, "is_secret": False}],
           "variables_scope": "local"},
          10, [{"target": 0, "custom_handle": None}], 6, "Count silent feeds + build summary."),
        A("condition", "core_action",
          {"name": "Any Silent Feeds", "action_type": "condition", "condition_type": "multi", "condition": None,
           "conditions": [{"input_value": "{{local_var.rowcount}}", "compared_value": "0", "comparison_operator": "greater_than"}],
           "conditions_relationship": "and"},
          0, [{"target": 14, "custom_handle": "true"}, {"target": 1, "custom_handle": "false"}], 6, "Alert if any silent."),
        A("variable", "core_action",
          {"name": "Set UIDs", "action_type": "variable", "variables": [
              {"name": "ind_uid", "value": "{{Function.GENERATE_UUID4()}}", "should_use_as_output": False, "is_secret": False},
              {"name": "alert_uid", "value": "{{Function.GENERATE_UUID4()}}", "should_use_as_output": False, "is_secret": False},
              {"name": "device_uid", "value": "{{Function.GENERATE_UUID4()}}", "should_use_as_output": False, "is_secret": False}],
           "variables_scope": "local"},
          14, [{"target": 15, "custom_handle": None}], 6, "Shared UIDs: indicator.metadata.uid == alert.related_events[].uid (stitch key)."),
        A("variable", "core_action",
          {"name": "Set Indicator", "action_type": "variable", "variables": [
              {"name": "Indicator", "value": indicator_payload, "should_use_as_output": False, "is_secret": False}],
           "variables_scope": "local"},
          15, [{"target": 16, "custom_handle": None}], 6, "Build the OCSF security_indicator (class_uid 1001) JSON string."),
        A("variable", "core_action",
          {"name": "CreateIndicatorFile", "action_type": "variable", "variables": [
              {"name": "indfile",
               "value": '{"file":[{"name":"Indicator.json","data": {{Function.STRING(local_var.Indicator)}} }]}',
               "should_use_as_output": False, "is_secret": False}],
           "variables_scope": "local"},
          16, [{"target": 17, "custom_handle": None}], 6, "Stage indicator as a files-array for gzip."),
        A("http_request", "integration",
          http("Create Indicator Context", "post", (hec_url or "{{HEC_URL}}") + "/v1/indicators",
               '{{Function.BASE64_DECODE_AS_BYTES(Function.COMPRESS(local_var.indfile.file, "gzip"))}}',
               {"Content-Type": "application/json", "Content-Encoding": "gzip", "S1-Scope": hec_scope}, "", True),
          17, [{"target": 3, "custom_handle": None}], 6, "gzip + POST the indicator to /v1/indicators."),
        A("delay", "core_action",
          {"name": "Indicator Settle Delay", "action_type": "delay", "time_unit": "seconds", "value": 3},
          3, [{"target": 12, "custom_handle": None}], 6, "Let the indicator uid register before the alert."),
        A("variable", "core_action",
          {"name": "Set SILENT Alert", "action_type": "variable", "variables": [
              {"name": "SilentAlert", "value": alert_payload, "should_use_as_output": False, "is_secret": False}],
           "variables_scope": "local"},
          12, [{"target": 13, "custom_handle": None}], 6, "Resolve the OCSF S1 Security Alert JSON into a string var."),
        A("variable", "core_action",
          {"name": "CreateAlertFile", "action_type": "variable", "variables": [
              {"name": "alertfile",
               "value": '{"file":[{"name":"Alert.json","data": {{Function.STRING(local_var.SilentAlert)}} }]}',
               "should_use_as_output": False, "is_secret": False}],
           "variables_scope": "local"},
          13, [{"target": 11, "custom_handle": None}], 6, "Stage alert as a files-array so COMPRESS gets a real file object."),
        A("http_request", "integration",
          http("Create SILENT Alert", "post", (hec_url or "{{HEC_URL}}") + "/v1/alerts",
               '{{Function.BASE64_DECODE_AS_BYTES(Function.COMPRESS(local_var.alertfile.file, "gzip"))}}',
               {"Content-Type": "application/json", "Content-Encoding": "gzip", "S1-Scope": hec_scope,
                "Authorization": "Bearer " + (hec_token or "{{HEC_TOKEN}}")}, "", False),
          11, [{"target": 1, "custom_handle": None}], 6, "gzip + POST S1 Security Alert to UAM /v1/alerts."),
        A("break_loop", "core_action", {"name": "Break When Done", "action_type": "break_loop"},
          1, [], 6, "Exit loop."),
    ]
    _bind_connection(actions, p.get("sdlIntegrationId"), p.get("hecIntegrationId"))
    _wf_desc = (f"Ingest-health SILENT watchdog for {src}. Daily anti-join LRQ: baseline {p['baselineTable']} "
                "LEFT JOIN last-24h live volume per entity; flags established feeds with ZERO live events and "
                "posts one OCSF S1 Security Alert per feed. The scheduled-detection engine runs on an aggregated "
                "data layer (no left join / dataset), so SILENT runs as an HA LRQ. Bind 'SentinelOne SDL' (Bearer) "
                "before activating.")
    return {"name": f"{p['prefix']} - {src} {lvl} ingest health SILENT",
            "description": _wf_desc, "actions": actions}


# --------------------------------------------------------------- baseline refresh HA flow (per level)
def refresh_workflow(p):
    """Per-level refresh flow: rebuilds ONE level's ingest-volume baseline (one nolimit savelookup,
    launched then polled to completion). `p` is a level view. The device and source levels each get
    their OWN refresh flow + table; they are staggered nightly (source 02:00, device 03:00 UTC) so
    the two `| nolimit` savelookups never run at the same time (only one is allowed per account)."""
    hrs = int(p.get("baselineHours", 720))
    lvl = _level_word(p)
    LVL = lvl.upper()
    hour = 3 if lvl == "device" else 2
    table, q = p["baselineTable"], savelookup_pq(p)
    def payload(q):
        return ('{\n  "queryType": "PQ",\n  "tenant": true,\n'
                '  "startTime": "{{Function.DELTA_NOW(' + str(hrs) + ')}}",\n'
                '  "endTime": "{{Function.DATETIME_NOW()}}",\n  "queryPriority": "HIGH",\n'
                '  "pq": {\n    "query": ' + _json_str(q) + ',\n    "resultType": "TABLE"\n  }\n}')

    A = lambda t, data, eid, conn, parent=None, desc="", tag="core_action", conn_name=None: {
        "action": {"type": t, "tag": tag, "connection_id": None, "connection_name": conn_name,
                   "use_connection_name": False, "integration_id": None, "data": data, "state": "active",
                   "description": desc, "client_data": {"position": {"x": 0, "y": 0},
                   "dimensions": {"width": 256, "height": 100}, "collapsed": False},
                   "snippet_workflow_id": None, "snippet_version_id": None},
        "export_id": eid, "connected_to": conn, "parent_action": parent}

    def http(name, method, url, payload_str, headers, desc):
        return {"name": name, "action_type": "http_request", "public_action_id": None, "method": method,
                "url": url, "url_path": None, "url_prefix": None, "payload": payload_str, "parameters": [],
                "retry_on_status_codes": [500], "ssl_verification": True, "timeout": 90, "headers": headers,
                "use_authentication_data": True, "use_proxy": False, "redirect_follow": True,
                "continue_on_fail": True, "body_type": "json"}

    b = 1000
    launch, setref, loop, poll, cond, brk, dly = b, b + 1, b + 2, b + 3, b + 4, b + 5, b + 6
    actions = [
        A("scheduled_trigger",
          {"name": "Scheduled Trigger", "action_type": "scheduled_trigger", "schedule_method": "daily",
           "until": None, "max_runs": 1,
           "schedule_value": [{"schedule_method": "daily", "minute": 0, "hour": hour, "tz": "UTC"}],
           "start_at": None, "start_at_method": "immediately", "ends_on": "never"},
          1, [{"target": launch, "custom_handle": None}], None, f"Run daily at {hour:02d}:00 UTC."),
        A("http_request",
          http("Launch baseline", "post", "{{Connection.protocol}}{{Connection.url}}/sdl/v2/api/queries",
               payload(q), {"Content-Type": "application/json", "Accept": "application/json"},
               f"Launch {table} savelookup (nolimit) LRQ."),
          launch, [{"target": setref, "custom_handle": None}], None, "", "integration", ""),
        A("variable",
          {"name": "Set Refs", "action_type": "variable", "variables": [
              {"name": "qid", "value": "{{launch-baseline.body.id}}", "should_use_as_output": False, "is_secret": False},
              {"name": "tag", "value": "{{Function.JQ(launch-baseline.headers, \"to_entries | map(select(.key|ascii_downcase==\\\"x-dataset-query-forward-tag\\\")) | .[0].value\", true)}}", "should_use_as_output": False, "is_secret": False}],
           "variables_scope": "local"},
          setref, [{"target": loop, "custom_handle": None}], None, "Capture LRQ id + forward tag."),
        A("loop",
          {"name": "Poll baseline", "action_type": "loop", "loop_type": "while", "number_of_iterations": "60",
           "object_to_iterate": "", "is_parallel": False},
          loop, [{"target": poll, "custom_handle": "inner"}], None, "Poll until the baseline savelookup completes."),
        A("http_request",
          http("Poll baseline LRQ", "get",
               "{{Connection.protocol}}{{Connection.url}}/sdl/v2/api/queries/{{local_var.qid}}?lastStepSeen=0",
               None, {"Accept": "application/json", "X-Dataset-Query-Forward-Tag": "{{local_var.tag}}"},
               "Poll the LRQ."),
          poll, [{"target": cond, "custom_handle": None}], loop, "", "integration", ""),
        A("condition",
          {"name": "baseline done", "action_type": "condition", "condition_type": "multi", "condition": None,
           "conditions": [{"input_value": "{{poll-baseline-lrq.body.stepsCompleted}}", "compared_value": "{{poll-baseline-lrq.body.totalSteps}}", "comparison_operator": "equals"}],
           "conditions_relationship": "and"},
          cond, [{"target": brk, "custom_handle": "true"}, {"target": dly, "custom_handle": "false"}], loop, "Done?"),
        A("break_loop", {"name": "Break", "action_type": "break_loop"}, brk, [], loop, "Exit loop."),
        A("delay", {"name": "Delay", "action_type": "delay", "time_unit": "seconds", "value": 5}, dly, [], loop, "Wait, then re-poll."),
    ]
    _bind_connection(actions, p.get("sdlIntegrationId"))
    return {"name": f"{p['prefix']} {p.get('source') or 'sources'} {LVL} Ingest Baseline Refresh",
            "description": (f"Rebuild of the {lvl}-level ingest-volume baseline {table} over the trailing {hrs}h "
                            f"(one nolimit savelookup). Nightly at {hour:02d}:00 UTC and once at deploy via run-now. "
                            "Bind the 'SentinelOne SDL' (Bearer) connection before activating."),
            "actions": actions}


# --------------------------------------------------------------- helpers
def _json_str(s):
    import json
    return json.dumps(s)


def dashboard_json(p):
    import dashboard as _dash
    return _dash.review_dashboard_json(p)
