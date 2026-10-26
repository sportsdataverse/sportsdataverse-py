- **CBS football adapter:** a failed scoreboard page (a dead connection, a 403/5xx after retries) is read again after
  5 minutes instead of being remembered as "no games" for the life of the process. (#922)
- **CBS football adapter:** a failed (non-404) optional NAPI resource raises instead of reading as absent, and the NFL
  adapter does not fetch CBS's odds when the caller already supplies the line. (#922)
