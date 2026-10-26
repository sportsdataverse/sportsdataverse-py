- **Codegen:** flat-API endpoints take `body: query|form|json` and per-param `in: query|body`; CBA, KHL REST and
  HockeyAllsvenskan use it, with a curl example in the docs. A caller `params=` entry that names a body field overrides
  the body; any other `params=` entry goes to the query string. (#862)
