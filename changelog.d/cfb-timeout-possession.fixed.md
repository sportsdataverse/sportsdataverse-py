- **CFB:** a Timeout row (filed under the team that called it) is no longer a change of possession: the next play reads
  the last real play's possession and score, and a play's `wp_after` the next real board. (#834)
- **CFB:** penalty rows after a defensive timeout lose a phantom `EP_between`, so their EPA moves by up to 6.2 points;
  `firstD_by_*` flags and WP columns move with it, and `lead_wp_before2` skips timeouts to the second real row. (#834)
