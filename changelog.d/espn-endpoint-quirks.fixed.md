- **ESPN:** an ESPN 400 saying a resource is "not supported" (probabilities for an NHL, soccer or
  unstarted game; all-time leaders outside the four pro leagues) raises `NoDataError`, not
  `ValueError`. The scoreboard `dates` docs no longer advertise ranges, which ESPN rejects. (#PR)
