- **HTTP chokepoints:** a 2xx JSON `null`, number, string or boolean body raises `AssetFetchError` instead of reaching a
  parser as data (or `{}`), except OpenLigaDB's `/getlastchangedate`, which answers a bare ISO string by design. (#880)
- **HTTP chokepoints:** a 2xx `{"code": 400-599, "message"}` envelope is classified by its code (404 `NoDataError`,
  400/422 `ValueError`, else `AssetFetchError`); barttorvik and the Savant CSV leaderboards refuse an HTML page as
  `AssetFetchError`; Yahoo and On3 data routes refuse a JSON array where an object belongs. (#880)
- **`download()`:** a 403 from api.nfl.com, premium.pff.com, comp.uefa.com, kenpom.com or herhoopstats.com is not
  retried; a malformed URL (`MissingSchema`, `InvalidURL`, `InvalidHeader`) raises at once instead of retrying ~50 s.
  (#880)
- **`download()`:** retry waits share a 180 s budget per call and a wait over 10 s logs a WARNING; a 2xx error envelope
  or JSON scalar is never written to the response cache. (#880)
