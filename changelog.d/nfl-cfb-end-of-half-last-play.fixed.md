- **NFL:** `end_of_half` is true on a finished game's last play and on the play that ends regulation, so a
  non-scoring game-ending play scores `EPA == -EP_start` and the last drive's `fixed_drive_result` fills; the
  2002-05 feeds' untyped "End of the game." row is no longer a play. (#832)
- **NFL, CFB:** a half or game that ends on a timeout row ends on the play before it, which now takes
  `end_of_half` and `EPA == -EP_start`; the timeout row took it (EPA 0.0). (#832)
