- **Docs and codegen:** every reference and "Additional functions" page now renders a **Raises**
  table from the function's docstring (generated wrappers: the endpoint YAML `raises:` plus the error
  vocabulary). The parameter table of a POST body endpoint (`body: form|json`: CBA, KHL, HockeyAllsvenskan)
  gains an **In** column (`path` / `query` / `body`). Pages that mostly list per-competition functions
  minted from the competition registry no longer call them "hand-written wrappers". An endpoint YAML can
  name a `body_builder:` (a runtime callable) that turns the flat body into the nested JSON an API wants,
  and the shared getter `_codegen_runtime._get` (through `dl_utils.download`, which now POSTs when given
  `data=` / `json=` and never caches a body request) sends form and JSON bodies, so a body endpoint no
  longer needs its own runtime getter. (#864)
