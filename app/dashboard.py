"""
Ingest-health review dashboard: an overview tab with rich visuals plus one tab per detection
(SILENT / DROP / SPIKE / NEW). review_dashboard_json(p) is called by the deployer.

SDL rules honoured: markdown panels use the "markdown" key; number panels end in
`| group ... | limit 1`; category bars use stacked_bar + xAxis grouped_data; heatmap uses the
documented transpose shape; time series use xAxis time.
"""
import json
import templates as T

# detection -> (title, one-line logic, metric column for the top-entities chart)
_META = {
    "silent": ("SILENT", "an established feed with ZERO events now (feed dark)", "baseline_avg"),
    "drop":   ("DROP", "24h volume far below the feed's baseline p05 (degraded, not zero)", "live_count"),
    "spike":  ("SPIKE", "24h volume far above the feed's baseline p95 (flood / loop / misconfig)", "live_count"),
    "new":    ("NEW", "ingesting now with no baseline entry (unexpected / first-seen feed)", "live_count"),
}


def _detq(p, kind):
    return T.antijoin_pq(p) if kind == "silent" else T._rule_pq(p, kind)


def _cnt(q):
    return q + " | group n = count() | limit 1"


def _chart(q, metric):
    # top entities by the detection metric, as a (category, value) bar. rsplit on the LAST
    # "| columns" so an anti-join subquery's own "| columns" stays intact.
    pre = q.rsplit("| columns", 1)[0]
    return pre + f"| sort -{metric} | columns entity_v, {metric} | limit 12"


def review_dashboard_json(p):
    ent = T.entity_field(p)
    base = T._base(p)
    scope_word = "device" if p.get("scope") == "device" else "source"
    tabs = []

    legend = ("**How to read this dashboard.** Live 24h ingest volume per "
              f"**{scope_word}** ({ent}) scored against the baseline `{p['baselineTable']}`. "
              f"Method **{p.get('method','robust')}**, granularity **{p.get('baselineGranularity','daily')}**. "
              "Each tab is one health detection.\n\n"
              "| Detection | Fires when |\n|---|---|\n"
              + "\n".join(f"| {m[0]} | {m[1]} |" for m in _META.values()))
    tabs.append({"tabName": "Overview", "graphs": [
        {"graphStyle": "markdown", "title": "Ingest health", "markdown": legend,
         "layout": {"w": 60, "h": 8, "x": 0, "y": 0}},
        {"graphStyle": "number", "title": f"Monitored {scope_word}s (24h)",
         "query": f"{base} {ent}=* | group n=estimate_distinct({ent}) | limit 1",
         "options": {"format": "commas", "precision": "0", "color": "#8b5cf6"},
         "layout": {"w": 15, "h": 8, "x": 0, "y": 8}},
        {"graphStyle": "number", "title": "Events (24h)",
         "query": f"{base} | group n=count() | limit 1",
         "options": {"format": "commas", "precision": "0", "color": "#42d6e8"},
         "layout": {"w": 15, "h": 8, "x": 15, "y": 8}},
        {"graphStyle": "number", "title": "Silent feeds now",
         "query": _cnt(T.antijoin_pq(p)),
         "options": {"format": "commas", "precision": "0", "color": "#ff4f9a"},
         "layout": {"w": 15, "h": 8, "x": 30, "y": 8}},
        {"graphStyle": "number", "title": "New / unexpected feeds",
         "query": _cnt(T._rule_pq(p, "new")),
         "options": {"format": "commas", "precision": "0", "color": "#37d39a"},
         "layout": {"w": 15, "h": 8, "x": 45, "y": 8}},
        {"graphStyle": "line", "title": "Ingest volume over time (24h)",
         "query": f"{base} | group events=count() by timestamp=timebucket('30m') | sort timestamp",
         "xAxis": "time", "lineSmoothing": "straightLines",
         "layout": {"w": 60, "h": 12, "x": 0, "y": 16}},
        {"graphStyle": "stacked_bar", "title": f"Top {scope_word}s by volume (24h)", "xAxis": "grouped_data",
         "query": f"{base} {ent}=* | group events=count() by {ent} | sort -events | limit 15",
         "layout": {"w": 30, "h": 14, "x": 0, "y": 28}},
        {"graphStyle": "heatmap", "title": f"Volume by {scope_word} and hour (UTC)", "xAxis": "grouped_data",
         "showDataLabels": "true", "colorScheme": "green", "colorSchemeOrder": "standard",
         "numberOfRanges": 5, "rangesCreation": "automatic",
         "query": f"{base} {ent}=* | let hod=strftime(timestamp,'%H') | group c=count() by entity={ent}, hod | transpose entity on hod",
         "layout": {"w": 30, "h": 14, "x": 30, "y": 28}},
    ]})

    for kind, m in _META.items():
        title, logic, metric = m
        q = _detq(p, kind)
        watchdog = kind == "silent"
        md = (f"**Fires when:** {logic}.  \n"
              f"**Mechanism:** {'Hyperautomation watchdog (anti-join LRQ, one OCSF alert per run)' if watchdog else 'scheduled detection, entity = ' + ent}.")
        tabs.append({"tabName": title, "graphs": [
            {"graphStyle": "markdown", "title": f"{title} - ingest health", "markdown": md,
             "layout": {"w": 60, "h": 5, "x": 0, "y": 0}},
            {"graphStyle": "number", "title": "Feeds flagged now", "query": _cnt(q),
             "options": {"format": "commas", "precision": "0", "color": "#ff4f9a"},
             "layout": {"w": 20, "h": 8, "x": 0, "y": 5}},
            {"graphStyle": "stacked_bar", "title": "Top feeds", "xAxis": "grouped_data",
             "query": _chart(q, metric), "layout": {"w": 40, "h": 8, "x": 20, "y": 5}},
            {"graphStyle": "table", "title": "Detail", "query": q,
             "layout": {"w": 60, "h": 14, "x": 0, "y": 13}},
        ]})

    return json.dumps({"configType": "TABBED", "duration": "24h",
                       "description": f"Ingest-health review for {p.get('source') or 'monitored sources'}, one tab per detection.",
                       "tabs": tabs}, indent=2)
