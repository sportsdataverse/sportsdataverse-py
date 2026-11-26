- **Codegen:** the autodoc schema capture's per-call timeout (`SDV_AUTODOC_CALL_TIMEOUT`) now holds on Windows
  and off the main thread too. It needed SIGALRM, so there one hung call stalled the whole capture; it now stops
  waiting for that call on a daemon thread and moves on. (#1324)
