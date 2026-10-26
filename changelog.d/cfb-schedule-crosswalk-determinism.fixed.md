- **CFB crosswalks:** `cfb_schedule_crosswalk` builds the same rows from the same upstream data. A Yahoo week that
  answered HTTP 200 with a cut-off JSON body was skipped, so its Yahoo ids and Yahoo-only games came and went between
  nightly builds. (#812)
- **HTTP:** `_download_json` (and the Yahoo Shangrila getter) re-fetches a truncated JSON body up to twice. (#812)
- **CFB crosswalks:** a full-season build raises `CrosswalkSourceError` on any failed ESPN calendar, ESPN week or Yahoo
  week instead of skipping it. (#812)
