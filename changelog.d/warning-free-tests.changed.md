- **Tests:** warnings are errors. A test asserts an expected warning with `pytest.warns(..., match=...)` or
  filters an incidental one by exact message; live tests skip on a timeout or upstream 429/5xx, the
  live job runs on Ubuntu, and the `tests` extra needs pytest >= 8.0. (#726)
