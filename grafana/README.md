# Grafana + Parquet PoC

This directory answers a feasibility question for Zecca:

> Can Grafana read our Parquet data, and how hard is a candlestick dashboard?

**TL;DR** — Grafana has **no built-in Parquet datasource**, but integration is
**low effort** using the **DuckDB datasource plugin**, which queries our
hive-partitioned parquet (or the existing `storage.duckdb`) with plain SQL and
feeds Grafana's **native candlestick panel**. This folder is a working,
reproducible PoC via Docker Compose.

---

## 1. Can Grafana be integrated with Parquet? — Findings

**Not natively.** Grafana core ships no Parquet datasource. The realistic options:

| Option | Reads local Parquet? | SQL / time-range filtering | Fit for Zecca |
|---|---|---|---|
| **DuckDB datasource** (`motherduck-duckdb-datasource`) | ✅ `read_parquet()` | ✅ full SQL + `$__timeFilter` | **Best** — also opens our `storage.duckdb` directly |
| Infinity (`yesoreyeram-infinity-datasource`) | ⚠️ only over HTTP, not local paths | ❌ no SQL (client-side only) | Weak |
| ClickHouse / Trino in front of parquet | ✅ | ✅ | Overkill — extra always-on service |
| Convert parquet → Postgres/Prometheus | n/a | ✅ | Defeats the point of parquet |

**Why DuckDB wins for us:**
- Our medallion store already uses DuckDB (`etl/dbt/profiles.yml` → `storage.duckdb`)
  and hive-partitioned parquet (`Model.store`). DuckDB reads both with zero conversion.
- `candles_daily` already exposes `open/high/low/close/volume`, the exact column
  names Grafana's candlestick panel **auto-detects**. Only `timeframe`→time is mapped.
- Full SQL means dashboard-side filtering by `symbol`, date range, and aggregation.

### Effort to enable
**Low — roughly half a day**, no code changes to the pipeline:
1. Run Grafana with the DuckDB plugin (unsigned → one allowlist env var; needs the
   `-ubuntu` image, not Alpine). *(done in `docker-compose.yml`)*
2. Provision the datasource in in-memory mode and query parquet via `read_parquet`,
   **or** set `path: /data/storage.duckdb` to query the real DuckDB store. *(done)*
3. Build the dashboard. *(done — `provisioning/dashboards/zecca-candles.json`)*

The only real caveats: the plugin is **unsigned** (community-maintained by MotherDuck)
and **single-process** (DuckDB is embedded, not a shared server) — fine for a PoC /
single-analyst dashboard, worth revisiting if many concurrent viewers are needed.

## 2. Candlestick dashboard complexity — Low

Grafana has had a **first-class candlestick panel since v8.3**. With OHLC columns
already named correctly, the entire panel query is:

```sql
SELECT timeframe AS "time", open, high, low, close, volume
FROM read_parquet('/data/candles_daily/**/*.parquet', hive_partitioning = true)
WHERE symbol = '$symbol' AND $__timeFilter(timeframe)
ORDER BY timeframe
```

No transforms, no field renaming, no custom plugin. A `$symbol` template variable
turns it into a per-ticker explorer. Adding overlays we already compute
(`open_rolling_*`, RSI, volatility) is just extra `SELECT` columns / panels.

---

## How to run

```bash
# 1. generate sample parquet (matches the candles_daily schema)
.venv/Scripts/python grafana/seed_sample_data.py

# 2. start Grafana (first run downloads the plugin — needs internet)
cd grafana
docker compose up

# 3. open http://localhost:3000  (admin / admin)
#    Dashboards → "Zecca — Candlestick PoC"
```

## Pointing at real pipeline data

The dashboard query is schema-compatible with the real `silver/candles_daily`
output. Two ways to switch from sample data to real data:

- **Parquet:** mount the real store and update the glob — in `docker-compose.yml`
  replace `./data:/data:ro` with `../dataplatform:/data:ro`, then point the query
  at `/data/silver/candles_daily/**/*.parquet`.
- **DuckDB file:** set `jsonData.path: /data/storage.duckdb` in
  `provisioning/datasources/duckdb.yml`, mount `../dataplatform/storage.duckdb`,
  and query `FROM candles_daily` directly.

To make `$symbol` dynamic instead of the hardcoded sample list, change the
`symbol` template variable to a *Query* variable on the DuckDB datasource:

```sql
SELECT DISTINCT symbol FROM read_parquet('/data/candles_daily/**/*.parquet', hive_partitioning = true) ORDER BY symbol
```

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Grafana + DuckDB plugin, provisioned |
| `seed_sample_data.py` | Generates sample OHLCV parquet (gitignored `data/`) |
| `provisioning/datasources/duckdb.yml` | DuckDB datasource (in-memory) |
| `provisioning/dashboards/*.yml` / `*.json` | Dashboard provider + candlestick dashboard |

## Sources
- DuckDB datasource: https://github.com/motherduckdb/grafana-duckdb-datasource
- Candlestick panel docs: https://grafana.com/docs/grafana/latest/panels-visualizations/visualizations/candlestick/
- Infinity datasource (local-file limitation): https://github.com/grafana/grafana-infinity-datasource/discussions/129
