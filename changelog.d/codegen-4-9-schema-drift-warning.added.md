- **Errors:** `sportsdataverse.errors.SchemaDriftWarning`. The NHL api-web / EDGE / Stats REST / Records, MLB Stats API,
  stats.nba.com and ESPN parsers still return zero rows for a payload whose shape they do not recognise, but now warn
  `SchemaDriftWarning` naming the parser and payload path, so an upstream schema change no longer reads as "no data".
  `warnings.filterwarnings("error", category=SchemaDriftWarning)` turns it into a failure. (A-L60)
