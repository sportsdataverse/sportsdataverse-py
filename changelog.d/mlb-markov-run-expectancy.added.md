- **MLB:** `mlb_base_out_transitions` counts base-out transitions between plate appearances (optionally keyed by outcome
  with `by_event=True`), and `mlb_markov_run_expectancy` solves them as an absorbing Markov chain into an RE24 matrix
  shaped like `mlb_run_expectancy_matrix`. (#1277)
