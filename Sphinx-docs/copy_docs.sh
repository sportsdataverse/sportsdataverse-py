#!/bin/bash
make markdown
cp _build/markdown/sportsdataverse.cfb.md ../docs/docs/cfb/index.md
cp _build/markdown/sportsdataverse.mbb.md ../docs/docs/mbb/index.md
cp _build/markdown/sportsdataverse.nba.md ../docs/docs/nba/index.md
cp _build/markdown/sportsdataverse.nfl.md ../docs/docs/nfl/index.md
cp _build/markdown/sportsdataverse.nhl.md ../docs/docs/nhl/index.md
cp _build/markdown/sportsdataverse.wbb.md ../docs/docs/wbb/index.md
cp _build/markdown/sportsdataverse.wnba.md ../docs/docs/wnba/index.md
cp ../CHANGELOG.md ../docs/src/pages/CHANGELOG.md