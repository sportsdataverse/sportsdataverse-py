<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [socceraction oracle fixtures](#socceraction-oracle-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# socceraction oracle fixtures

| File | What | Source |
|---|---|---|
| `8658_spadl.csv` | SPADL actions for StatsBomb open-data match 8658 (2018 World Cup final) from socceraction 1.5.3's DIRECT StatsBomb converter (`spadl.statsbomb.convert_to_actions` + `play_left_to_right` + `add_names`). Every action attacks left to right on the 105 x 68 pitch. | frozen 2026-10-07 by `tools/models/freeze_socceraction_oracle.py` |
| `8658_xt_fit.json` | `ExpectedThreat(l=16, w=12).fit(actions)` on those actions alone, written by `save_model` (a bare 12 x 16 nested list; row 0 = top of the pitch). | same run |
| `8658_xt_fit_log.txt` | versions and the iteration count socceraction printed. | same run |

The script runs only in a throwaway venv, installed with
`uv pip install --python <venv>/bin/python "socceraction==1.5.3" "numpy<2" "statsbombpy>=1.13" "multimethod<2"` (on Windows the interpreter is `<venv>/Scripts/python.exe`)
(`multimethod<2` is required: pandera 0.17.2 fails to import against multimethod 2.x). Resolved versions: socceraction 1.5.3,
numpy 1.26.4, multimethod 1.12, pandera 0.17.2, python 3.12.6. socceraction's pins are incompatible with sdv-py's lock. The oracle is the direct converter, not socceraction's kloppy converter, because
the latter requests `Orientation.HOME_AWAY`, which flips the home side each period in kloppy 3.19.

Licenses: socceraction is MIT (c) 2019 KU Leuven Machine Learning Research Group. StatsBomb open data
(<https://github.com/statsbomb/open-data>) is free for research and non-commercial use under StatsBomb's public-data license.
