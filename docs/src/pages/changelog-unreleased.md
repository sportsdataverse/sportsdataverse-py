---
title: Unreleased changes
---

# Unreleased changes

Merged to `main` since 0.1.5 and not yet released. Released versions are on the [Changelog](/CHANGELOG).

## Unreleased

### Changed

- **Tests:** warnings are errors. A test asserts an expected warning with `pytest.warns(match=)` or
  filters an incidental one by exact message; live tests skip on a timeout or upstream 429/5xx, and the
  live job runs on Ubuntu. (#726)

### Fixed

- **CFB:** `get_go_wp` returns NaN for a 4th-down row with no `yards_to_goal` or `distance`, instead of
  a garbage go-for-it value from casting NaN to an integer; other plays are unchanged. (#726)
- **CFB:** scoring zero rows (QBR on a live game's opening drive, `predict_from_card` on an empty frame)
  no longer logs XGBoost's "Empty dataset" warning. (#726)
- **NBA:** `logistic_fit_irls` no longer overflows on separable data, and `nba_rapm` fits a single
  possession without a divide-by-zero warning. (#726)
- **NFL:** `get_go_wp` on a full nflverse frame no longer raises pandas `PerformanceWarning`s; output is
  unchanged. (#726)
