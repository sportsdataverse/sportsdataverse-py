- **NBA / WNBA stats:** `SDV_PY_NBA_STATS_PROXY` routes every `nba_stats_*` / `wnba_stats_*` call through a proxy
  (`http://user:pass@host:port`) when the call passes no `proxy_url`; `proxy_url=""` bypasses it. stats.nba.com and
  stats.wnba.com hang on cloud IPs, so the `tutorials-refresh` workflow sets it from a proxy pool. (#1017)
