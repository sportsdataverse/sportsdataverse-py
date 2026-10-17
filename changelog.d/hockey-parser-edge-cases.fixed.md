- **Hockey:** a `{"data": null}` body parses to a zero-row frame (it was one all-null row) in the generic
  KHL webcaster, Liiga and National League parsers; `khl_playoff_bracket.game_ids` is `List(Int64)` (it was a comma-joined string).
