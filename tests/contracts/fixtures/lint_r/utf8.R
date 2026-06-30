library(dplyr)

# Non-ASCII content — “smart quotes”, an em-dash —, and an accented name José —
# exercises the UTF-8 decode path in _parse_data_csv (Windows cp1252 default
# would UnicodeDecodeError and silently drop this file instead of linting it).
bad <- df |>
  mutate(prev_ep = lag(ep))
