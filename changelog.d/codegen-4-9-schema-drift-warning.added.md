- **Errors:** `sportsdataverse.errors.SchemaDriftWarning`. The NHL api-web / EDGE / Stats REST / Records, MLB Stats
  API, stats.nba.com and ESPN parsers still return zero rows for a payload shape they do not recognise, but warn naming
  the parser and payload path, so an upstream schema change no longer reads as "no data". (#907)
- **Errors:** `warnings.filterwarnings("error", category=SchemaDriftWarning)` turns schema drift into a failure. (#907)
