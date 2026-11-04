- **Hockey / basketball:** every season-keyed SHL and Basketligaen call failed (SHL: "no ssgt directory in the
  body"; Basketligaen: "0 ssgts in the body, expected the 40+ of 25 seasons"): both sites renamed the
  `/statistics-v2/layout-info` key `seasonSeriesGameTypeTeams` to `originalSeasonSeriesGameTypeTeams`. The new
  key is read now, with the old one as a fallback (#1083)
