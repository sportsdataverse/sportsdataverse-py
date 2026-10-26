- **Docs:** reference and "Additional functions" pages render a **Raises** table from the function's docstring, one
  row per exception and never a `None` row; generated wrappers (including `nba_stats` / `wnba_stats`) list their
  runtime's error vocabulary plus any endpoint YAML `raises:` qualifier. (#864)
- **Docs:** a parser's "empty payload gives a zero-row frame" note moves to its Returns, and the parameter table of a
  POST body endpoint (`body: form|json`: CBA, KHL, HockeyAllsvenskan) gains an **In** column (`path` / `query` /
  `body`). (#864)
- **Docs:** pages that mostly list per-competition functions minted from the competition registry no longer call them
  "hand-written wrappers", and a split family page never ends on a lone function. (#864)
- **Codegen:** an endpoint YAML can name a `body_builder:` (a runtime callable) that turns the flat body into the nested
  JSON an API wants; only top-level `None` fields are dropped, so a builder handles `None` inside what it nests. (#864)
- **HTTP:** `dl_utils.download` POSTs when given `data=` / `json_body=` and never caches a body request, so
  `_codegen_runtime._get` sends form and JSON bodies and a body endpoint needs no runtime getter of its own. (#864)
- **HTTP:** a POST through `download` is retried like a GET, so it is for idempotent read APIs only. (#864)
