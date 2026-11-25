- **polars 2.2:** the lock moves to polars 2.2 (2.0 on Python 3.10). ESPN soccer and cricket standings, NCAA box
  player stats and the Yahoo/Fox CFB frames keep a stat column first seen after row 100, where 2.2 raised and the
  parser returned an empty frame (2.0 dropped the column). (#1279)
