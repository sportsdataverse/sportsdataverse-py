- **Codegen:** flat-API endpoints take `body: query|form|json` (with `method: post`) and per-param
  `in: query|body`; the wrapper passes `params=` plus `data=` / `json=` to the family getter. CBA
  (JSON body, paging in the query), KHL REST (form) and HockeyAllsvenskan (JSON) use it, so the POST
  docs show a curl request instead of an unfetchable "Valid URL". Public signatures are unchanged.
