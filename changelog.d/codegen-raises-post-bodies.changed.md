- **Docs and codegen:** reference and "Additional functions" pages now render a **Raises** table from the
  function's docstring (generated wrappers: the error vocabulary of their runtime, with any endpoint YAML `raises:`
  qualifier added to the same row; the `nba_stats` / `wnba_stats` wrappers included). The table has one row per
  exception, never a `None` row (a parser's "empty payload gives a zero-row frame" note now sits in its Returns).
  The parameter table of a POST body endpoint (`body: form|json`: CBA, KHL, HockeyAllsvenskan) gains an **In**
  column (`path` / `query` / `body`). Pages that mostly list per-competition functions minted from the competition
  registry no longer call them "hand-written wrappers", and a split family page never ends on a lone function. An
  endpoint YAML can name a `body_builder:` (a runtime callable, imported from the module it lives in) that turns
  the flat body into the nested JSON an API wants; only top-level `None` fields are dropped, so a builder handles
  `None` inside what it nests. The shared getter `_codegen_runtime._get` (through `dl_utils.download`, which now
  POSTs when given `data=` / `json_body=` and never caches a body request) sends form and JSON bodies, so a body
  endpoint no longer needs its own runtime getter. A POST through `download` is retried like a GET, so it is for
  idempotent read APIs only. (#864)
