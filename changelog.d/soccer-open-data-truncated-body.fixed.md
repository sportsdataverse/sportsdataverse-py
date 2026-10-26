- **Soccer:** `soccer_open_dataset` / `soccer_open_events` retry a truncated StatsBomb events file once,
  after evicting the partial download kloppy cached (only that file; kloppy's cache setting is untouched),
  then raise `AssetFetchError` naming the URL instead of a raw `JSONDecodeError`. (#1014)
