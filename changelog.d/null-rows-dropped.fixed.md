- **Parsers:** a `null` row in a row list is dropped wherever it sits, instead of becoming an all-null row, raising,
  or (as the first row) changing the frame's shape; a list holding only nulls reads as an empty one. Any other row
  that is not an object is still schema drift. Covers the ESPN, NHL and MLB Stats API parsers. (#953)
