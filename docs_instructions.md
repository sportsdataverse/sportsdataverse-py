## Documentation update
```bash
bash create_docs.sh
```
Don't forget to include any new modules in the [sidebars](https://github.com/sportsdataverse/sportsdataverse-py/blob/d1fd11e367d8cf180bb11ea2c676295771434225/docs/sidebars.js) for the docusaurus config

## Documentation Creation
```bash
pip install sphinx sphinx-markdown-builder;
```
```bash
sphinx-apidoc -o Sphinx-docs . sphinx-apidoc --full -A 'Saiem Gilani';
cd Sphinx-docs;
```

### Conf.py
```bash
echo "
import os
import sys
sys.path.insert(0,os.path.abspath('../'))
def skip(app, what, name, obj,would_skip, options):
    if name in ( '__init__',):
        return False
    return would_skip
def setup(app):
    app.connect('autodoc-skip-member', skip)
" >> conf.py;
```

```bash
make markdown;
```

At this point, you should have some combined markdown files in the `Sphinx-docs/_build/` folder

Some will be the functions you documented and those are the ones we are most interested in.

# Docusaurus(v2) Website

This website is built using [Docusaurus 2](https://docusaurus.io/), a modern static website generator.

## Installation

```console
yarn install
```

## Local Development

```console
yarn start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

## Build

```console
yarn build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

## Deployment

```console
GIT_USER=<Your GitHub username> USE_SSH=true yarn deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.
