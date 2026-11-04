- **Hockey / basketball:** every season-keyed SHL and Basketligaen call failed ("no ssgt directory in the body",
  "0 ssgts in the body"): both sites renamed the `/statistics-v2/layout-info` key `seasonSeriesGameTypeTeams` to
  `originalSeasonSeriesGameTypeTeams`. The new key is read now, with the old one as a fallback. (#1083)
