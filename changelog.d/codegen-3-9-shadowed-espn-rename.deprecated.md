- **ESPN:** the old names of those 12 wrappers in `*_espn_ext` (`from sportsdataverse.cfb.cfb_espn_ext import
  espn_cfb_calendar`, ...) still return the wrapper but warn `DeprecationWarning` naming the `*_site` name; they are
  removed in 0.4.0. (#903)
