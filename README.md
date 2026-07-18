# s1-ingest-health-deployer

> **Disclaimer.** This is a community-supported tool, not an official SentinelOne product and not
> covered by SentinelOne support. It uses **no AI or LLM and needs no bring-your-own-AI**: it is a
> thin configurator that stitches together native SentinelOne capabilities you already have (SDL
> baselines, scheduled detections, Hyperautomation, dashboards). Sibling of
> [`s1-ueba-deployer`](https://github.com/pmoses-s1/s1-ueba-deployer); it reuses the same chassis.

A one-click, Dockerised web UI that stands up **ingest-health monitoring** on any Singularity Data
Lake source. It baselines the **expected event volume** per source (or per device) over a trailing
window and deploys four health detections, a review dashboard, and a nightly baseline refresh, so a
broken, blind, or runaway feed surfaces as a SentinelOne alert instead of a silent gap.

Broken and blind feeds are a top SOC blind spot: a source that quietly stops ingesting means every
detection built on it is silently dead. This tool watches for exactly that.

## What it deploys

Detections (grouped by **entity** = `dataSource.name`, or a device field for device-level scope):

| Detection | Fires when | Mechanism |
|---|---|---|
| **SILENT** | an established feed produces **zero** events now (feed dark / broken / blind) | Hyperautomation watchdog (anti-join LRQ → OCSF alert) |
| **DROP** | volume far **below** baseline but not zero (feed degraded) | scheduled detection |
| **SPIKE** | volume far **above** baseline (loop / misconfig / flood; also an ingest-cost signal) | scheduled detection |
| **NEW** | a feed ingesting now with **no baseline** (unexpected / first-seen feed) | scheduled detection |

Plus: a per-entity ingest-volume **baseline** (SDL datatable), a tabbed **review dashboard**, and a
nightly **baseline-refresh** Hyperautomation flow (also run once at deploy).

Why SILENT is a watchdog and not a scheduled rule: the scheduled-detection engine runs on a
pre-aggregated data layer with no `left join` / `dataset`, and SILENT needs the baseline datatable
joined to live volume. So SILENT runs as a Hyperautomation LRQ that posts one OCSF S1 Security Alert
per dark feed, the same pattern the UEBA deployer uses.

## Features

- **Source or device scope.** Baseline per `dataSource.name`, or per device within chosen sources.
- **Watch every source by default, with a source-exclusion list.** The common setup: monitor all sources (leave the list empty) and drop known-good / intentionally-bursty feeds with a **source-exclusion list** (by `dataSource.name`). New feeds are covered automatically, no reconfiguring; only listed sources are ignored.
- **Or pick specific sources.** Live source discovery when connected.
- **Robust or standard thresholds**, and **daily or hourly** baseline granularity.
- **Inclusions** (allowlist to watch only specific feeds) as the inverse of exclusions.
- **Save artifacts.** Download every generated query, rule, HA flow (with embedded queries extracted), and the dashboard as a `.zip`, no tenant required.
- **Offline mode.** Configure and export without connecting.
- **Delete deployed config.** Prefix-scoped, deactivates flows first, cannot touch anything outside its naming scope.
- **Foolproof prefix**, **cool-off / re-alert suppression**, and a clear **warning when no SDL connection is bound** (the HA flows then need manual bind + activate in the Hyperautomation UI).

## Run the deployer (Docker)

### Step 1, run the published image

```bash
docker run --rm --pull always -p 127.0.0.1:8888:8788 ghcr.io/pmoses-s1/s1-ingest-health-deployer:latest
```

`--pull always` fetches the newest `:latest` on every start, so you always get the current build with
**no separate `docker pull`**.
The image is multi-arch (amd64 + arm64), so it runs natively on Apple Silicon. It uses port **8788**
(host **8888**), distinct from `s1-ueba-deployer` (8799/8899), so both can run side by side.

Publishing to `127.0.0.1:8888` (not `8888`) keeps the port reachable only from this machine, which
matters because the deployer drives privileged S1 API calls with your token and is unauthenticated
by default. To serve it to other hosts, opt in explicitly and require a token:

```bash
docker run --rm --pull always -p 8888:8788 -e INGEST_BIND_ALL=1 -e INGEST_AUTH_TOKEN=<strong-secret> \
  --env-file .env ghcr.io/pmoses-s1/s1-ingest-health-deployer:latest
# then open  http://<host>:8888/?token=<strong-secret>
```

The server refuses to start network-exposed without a token, and every request must carry it.

### Step 2a, configure credentials in the UI

Open **http://localhost:8888** and paste your MGMT Console URL + API token (and the SDL / ingest
fields for the dashboard and the SILENT alert) in the Connect panel. Nothing is written to disk.

### Step 2b, or preload credentials from a file

```bash
cp .env.example .env    # fill it in
docker run --rm --pull always -p 127.0.0.1:8888:8788 --env-file .env ghcr.io/pmoses-s1/s1-ingest-health-deployer:latest
```

It comes up already connected.

### No tenant? Use offline

Click **Use offline** on the Connect panel to configure and **Save artifacts** (download the zip)
without a tenant, then deploy the artifacts later.

## The three steps

1. **Sources & scope**: source level is always deployed; optionally add the device level. Choose which sources to monitor (all by default, with SentinelOne and Windows Event Logs excluded by default), and set the naming prefix.
2. **Configuration**: sensitivity, baseline window + granularity, SILENT floor, cadence, method, the HA connection, and optional exclusions/inclusions.
3. **Detections & deploy**: select SILENT / DROP / SPIKE / NEW and hit Enable (or Save artifacts).

Deploying is a **one-off**; ongoing tuning is done in the SentinelOne console. Every artifact carries
your prefix, so it is easy to find and can be removed as a set from the Danger zone https://github.com/pmoses-s1/s1-ingest-health-deployer/blob/main/docs/user-guide.md#removing-a-deployment.

## Credentials

| Field / key | Required for |
|---|---|
| `S1_CONSOLE_URL`, `S1_CONSOLE_API_TOKEN` | everything (the token also authenticates HEC ingest) |
| `SDL_XDR_URL` | dashboard + live source discovery |
| `SDL_CONFIG_WRITE_KEY`, `SDL_CONFIG_READ_KEY` | deploying / reading the dashboard and lookup tables |
| `S1_HEC_INGEST_URL` | the SILENT watchdog alert |

See `.env.example` for the full list.

## Run from source

```bash
python3 app/server.py     # serves http://localhost:8788
```

No third-party dependencies (Python 3.9+ standard library only).
