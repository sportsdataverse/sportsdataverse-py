- **Codegen:** endpoint YAML can declare path/query transforms (`transform: {offset: -1}`, `{fn: ..., with: [...]}`), a
  per-route `getter:`, typed `choices:` (int / date, checked typed by the wrapper) and a family `no_r_fallback:` flag.
  (#865)
