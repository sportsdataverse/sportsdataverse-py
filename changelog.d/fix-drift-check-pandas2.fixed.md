- **Parsers:** with pandas 2 installed, a row list that stops holding objects now warns `SchemaDriftWarning` and
  returns zero rows, as with pandas 3; it came back as an N x 0 frame with no warning. Covers the ESPN, NHL api-web
  / EDGE / Stats REST / Records and MLB Stats API parsers, and `parse_draft`'s inline rounds no longer raise. (#941)
