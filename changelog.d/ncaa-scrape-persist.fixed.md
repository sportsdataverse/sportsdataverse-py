- **`scrape.espn.persist.write_payload`** writes through a temp file and `os.replace`, so a crash mid-write no longer
  leaves a truncated `.json` that counts as captured and is never re-fetched; ESPN's `{code, message}` error body is
  refused like the other error shapes. (#924)
- **NCAA parse stage:** a family whose parser crashed is recorded in a `parse_errors` field of the game's JSON (it was
  an empty list, like a game with no shots), and the stage exits 1 when any game failed or carries one. (#924)
- **`espn_injuries_snapshot` / `espn_depthcharts_snapshot`** warn and keep the other leagues / teams when one fetch
  fails; one failure discarded the whole day, which cannot be re-taken. Every fetch failing still raises. (#924)
- **NCAA ESPN game crosswalk:** a crosswalk written in this process is read back instead of the cached empty
  answer from before it existed. (#924)
- **NCAA discover / roster sweeps:** a team whose page 404s is skipped without counting toward the "ban-suspect"
  abort after 5 failures in a row, as the capture stage already did. (#924)
