- **Soccer:** `soccer_open_dataset` / `soccer_open_events` retry a truncated StatsBomb events file once
  against a fresh cache (kloppy kept the partial download), then raise `AssetFetchError` naming the
  URL instead of a raw `JSONDecodeError`. (#1014)
