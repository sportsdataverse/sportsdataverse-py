- **Soccer:** retrying a cut-off StatsBomb open-data body now evicts the cached copy under every URL kloppy can
  have used: its live GitHub redirect (`hudl/open-data/refs/heads/...`) and its fallback form. Evicting one let the
  retry re-read the partial file when the two lookups disagreed; the eviction tests no longer hit the network. (#1259)
