- **CFB:** `cfb_adjusted_breakdowns` adds opponent-adjusted team breakdowns on the plays `cfb_adjusted_epa` scores (EPA
  on all downs, success rate, early downs, standard downs, first half; offense and defense), each shrunk toward the
  league mean by n / (n + k) for display, with the raw value and play count alongside. (#1371)
- **CFB:** a pre-registered split-half reliability rule (2014-2023) decided which sides publish the adjusted value;
  first-half EPA publishes the raw value with the same shrinkage. Fitter:
  `tools/fit_cfb_adjusted_breakdowns.py`. (#1371)
