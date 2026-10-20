- **Parsers:** 153 provider parsers (HockeyTech, NFL.com Shield, Fox, CBS, 247Sports, EuroLeague, Sleeper, NFL Pro, Kalshi,
  Polymarket, soccer, international basketball, European hockey) return zero rows for an error envelope, an empty `{}` record
  or a wrong-typed body; HockeyTech `parse_game_summary` adds its `game` stub row only with a `game_id` or data. (#TBD)
