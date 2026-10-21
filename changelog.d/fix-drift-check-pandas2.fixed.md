- **Parsers:** on pandas 2 (resolved on Python 3.10) a row list that stops holding objects now warns
  `SchemaDriftWarning` and returns zero rows, as on pandas 3; it came back as an N x 0 frame with no
  warning. Covers the ESPN, NHL api-web / EDGE / Stats REST / Records and MLB Stats API parsers. (#TBD)
