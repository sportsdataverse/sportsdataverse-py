- **stats.ncaa.org:** the vendor browser transport (`NCAA_VENDOR`) no longer leaks a 2.6 MB Chromium profile
  into the temp directory on every launch (about 11,000 a day on a busy scraper); stale `patchright_udd_*`
  leftovers are swept automatically once a day old. (#1068)
