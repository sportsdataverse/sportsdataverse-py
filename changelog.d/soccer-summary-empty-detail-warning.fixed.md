- **ESPN soccer:** `parse_soccer_summary` (every `espn_<league>_summary`) warns `UserWarning`, naming the event id, when
  a payload has a match header but empty `key_events`, `commentary` and `lineups`, instead of returning zero-row frames
  silently; ESPN lists usa.nwsl 2014-2018 but ships no detail for those seasons. (#742)
