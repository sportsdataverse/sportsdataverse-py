- **Parsers:** a `null` row in a row list is dropped instead of becoming an all-null row, and a list holding only
  nulls is zero rows instead of an N x 0 frame; any other row that is not an object is still schema drift. Covers
  the ESPN, NHL api-web / EDGE / Stats REST / Records and MLB Stats API parsers, and `parse_draft`'s rounds. (#TBD)
