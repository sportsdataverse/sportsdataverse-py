- **Python 3.9 dropped.** `requires-python` is now `>=3.10` (3.9 is end-of-life and HEAD no longer
  imported on it: `cfb/cfb_drive_summary.py` evaluates `set[int] | str | None` at def time). The lock
  now resolves polars 2.0 on every supported Python; `scrape/ncaa/canary.py` imports on 3.10. (#748)
