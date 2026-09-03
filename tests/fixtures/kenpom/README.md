# kenpom fixtures

| File | Provenance |
|---|---|
| `ratings_2025.trim.html` | `GET https://kenpom.com/index.php?y=2025`, captured 2026-09-02 with a live subscription. **Trimmed**: 3 of the page's 10 `<thead>` blocks and the first 8 `<tbody>` rows; nothing else modified. |

The trim keeps what the tests need and nothing more. KenPom is subscription
content, so only enough rows to pin parsing behaviour are committed.

Why 3 `<thead>` blocks: KenPom re-renders its 2-row header roughly every 40 data
rows as **10 separate `<thead>` elements in one table**, so `pandas.read_html`
reports a **20-level** MultiIndex on the live page. Three blocks reproduce the
same bug class (6 levels, group/name labels repeating non-adjacently) at a
fraction of the size. The rows include a seeded team (`"Duke 1"`) and a blank
separator row.
