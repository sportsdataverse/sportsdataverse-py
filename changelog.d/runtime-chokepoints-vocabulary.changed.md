- **HTTP chokepoints:** a 2xx JSON `null`, number, string or boolean body now raises `AssetFetchError` instead of
  reaching a parser as data (or `{}`); a 2xx `{"code", "message"}` error envelope is classified by its code
  (404 `NoDataError`, 400/422 `ValueError`, else `AssetFetchError`); barttorvik and the Savant CSV leaderboards
  refuse an HTML page as `AssetFetchError`; Yahoo and On3 data routes refuse a JSON array where an object belongs.
- **`download()`:** a 403 from api.nfl.com, premium.pff.com, comp.uefa.com, kenpom.com or herhoopstats.com is no
  longer retried; a malformed URL (`MissingSchema`, `InvalidURL`, `InvalidHeader`) raises at once instead of
  retrying for ~50 s; the waits between retries share a 180 s budget per call, and a wait over 10 s logs a WARNING.
