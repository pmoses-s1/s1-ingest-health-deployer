"""
Ingest-health review dashboard: ONE dashboard spanning every enabled level (source and/or device).
An overview tab with combined KPIs, then per-level detection tabs (e.g. "SRC SILENT", "DEV SILENT").
review_dashboard_json(p) is called by the deployer.

SDL rules honoured: markdown panels use the "markdown" key; number panels end in
`| group ... | limit 1`; category bars use stacked_bar + xAxis grouped_data; heatmap uses the
documented transpose shape; time series use xAxis time.
"""
import json
import templates as T

# detection -> (label, one-line logic, metric column for the top-feeds chart)
_META = {
    "silent": ("SILENT", "an established feed with ZERO events now (feed dark)", "baseline_avg"),
    "drop":   ("DROP", "24h volume far below the feed's baseline p05 (degraded, not zero)", "live_count"),
    "spike":  ("SPIKE", "24h volume far above the feed's baseline p95 (flood / loop / misconfig)", "live_count"),
    "new":    ("NEW", "ingesting now with no baseline entry (unexpected / first-seen feed)", "live_count"),
}


def _detq(v, kind):
    return T.antijoin_pq(v) if kind == "silent" else T._rule_pq(v, kind)


def _cnt(q):
    return q + " | group n = count() | limit 1"


def _chart(q, metric):
    pre = q.rsplit("| columns", 1)[0]
    return pre + f"| sort -{metric} | columns entity_v, {metric} | limit 12"


def review_dashboard_json(p):
    levels = [l for l in (p.get("levels") or ["source"]) if l in ("source", "device")] or ["source"]
    base = T._base(p)                          # scope = monitored sources (+ noise filter)
    tabs = []

    lvl_line = " and ".join(
        (f"**source** per `dataSource.name` (`{p.get('baselineTableSource')}`)" if lv == "source"
         else f"**device** per `{p.get('deviceField','device')}` (`{p.get('baselineTableDevice')}`)")
        for lv in levels)
    legend = ("**How to read this dashboard.** Live 24h ingest volume scored against the baselines. "
              f"Levels: {lvl_line}. Method **{p.get('method','robust')}**, granularity "
              f"**{p.get('baselineGranularity','daily')}**. Each tab below is one level+detection.\n\n"
              "| Detection | Fires when |\n|---|---|\n"
              + "\n".join(f"| {m[0]} | {m[1]} |" for m in _META.values()))
    tabs.append({"tabName": "Overview", "graphs": [
        {"graphStyle": "markdown", "title": "Ingest health", "markdown": legend,
         "layout": {"w": 60, "h": 9, "x": 0, "y": 0}},
        {"graphStyle": "number", "title": "Sources ingesting (24h)",
         "query": f"{base} dataSource.name=* | group n=estimate_distinct(dataSource.name) | limit 1",
         "options": {"format": "commas", "precision": "0", "color": "#8b5cf6"},
         "layout": {"w": 20, "h": 8, "x": 0, "y": 9}},
        {"graphStyle": "number", "title": "Events (24h)",
         "query": f"{base} | group n=count() | limit 1",
         "options": {"format": "commas", "precision": "0", "color": "#42d6e8"},
         "layout": {"w": 20, "h": 8, "x": 20, "y": 9}},
        {"graphStyle": "number", "title": "Silent feeds now (all levels)",
         "query": _cnt(T.antijoin_pq(T.level_view(p, levels[0]))),
         "options": {"format": "commas", "precision": "0", "color": "#ff4f9a"},
         "layout": {"w": 20, "h": 8, "x": 40, "y": 9}},
        {"graphStyle": "line", "title": "Ingest volume over time (24h)",
         "query": f"{base} | group events=count() by timestamp=timebucket('30m') | sort timestamp",
         "xAxis": "time", "lineSmoothing": "straightLines",
         "layout": {"w": 60, "h": 12, "x": 0, "y": 17}},
        {"graphStyle": "stacked_bar", "title": "Top sources by volume (24h)", "xAxis": "grouped_data",
         "query": f"{base} dataSource.name=* | group events=count() by dataSource.name | sort -events | limit 15",
         "layout": {"w": 30, "h": 14, "x": 0, "y": 29}},
        {"graphStyle": "donut", "title": "Volume share by source (24h)", "maxPieSlices": 10, "dataLabelType": "PERCENTAGE",
         "query": f"{base} dataSource.name=* | group events=count() by dataSource.name | sort -events | limit 10",
         "layout": {"w": 30, "h": 14, "x": 30, "y": 29}},
    ]})

    for lv in levels:
        v = T.level_view(p, lv)
        ent = T.entity_field(v)
        tag = "SRC" if lv == "source" else "DEV"
        for kind, m in _META.items():
            title, logic, metric = m
            q = _detq(v, kind)
            watchdog = kind == "silent"
            md = (f"**Level:** {lv} (entity = `{ent}`).  \n**Fires when:** {logic}.  \n"
                  f"**Mechanism:** {'Hyperautomation watchdog (anti-join LRQ, one OCSF alert per run)' if watchdog else 'scheduled detection'}.")
            tabs.append({"tabName": f"{tag} {title}", "graphs": [
                {"graphStyle": "markdown", "title": f"{tag} {title} - ingest health", "markdown": md,
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
                       "description": f"Ingest-health review for {p.get('source') or 'monitored sources'} "
                                      f"({', '.join(levels)} level{'s' if len(levels)>1 else ''}), one tab per level+detection.",
                       "tabs": tabs}, indent=2)
