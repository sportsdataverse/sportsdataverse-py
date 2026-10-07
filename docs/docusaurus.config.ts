import * as fs from 'fs';
import * as path from 'path';
import type {PrismTheme} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import registry from './src/data/leagues.json';

// Cap which doc versions are BUILT, derived from versions.json (which Docusaurus
// maintains newest-first) so the list never needs manual editing at release time:
// the rolling `current` tree plus the latest N release snapshots. Older snapshots
// always stay under versioned_docs/ in git; this only controls what's built/served.
//
// Default 3: the rolling `current`/`main` tree plus the latest 3 release snapshots
// = 4 versions built/served. This is the OOM-safe default on the production Vercel
// container (the `current + latest 3` shape only OOMed the *smaller* pre-upgrade
// container; production has headroom for 4). Rolling cap, so older snapshots stop
// building as versions.json grows — raise only with verified container headroom.
const VERSIONS_TO_KEEP = 3;
const allReleasedVersions: string[] = JSON.parse(
  fs.readFileSync(path.join(__dirname, 'versions.json'), 'utf-8'),
);
const builtVersions: string[] = [
  'current',
  ...allReleasedVersions.slice(0, VERSIONS_TO_KEEP),
];

// Preload the two self-hosted faces above the fold: Inter (body) and Barlow Condensed 700 (the page
// title). The hrefs must equal the @font-face urls in src/css/sdv-theme.css or the browser fetches twice.
const fontPreloads = ['inter-latin-wght-normal', 'barlow-condensed-latin-700-normal'].map((name) => ({
  tagName: 'link',
  attributes: {rel: 'preload', href: `/fonts/${name}.woff2`, as: 'font', type: 'font/woff2', crossorigin: 'anonymous'},
}));

// Code-block themes on the family surfaces (white / #111b2e). Every token color is a sportsdataverse.org
// token and clears 4.5:1 on its background; builtins, variables and properties stay the plain text color.
const sdvPrismLight: PrismTheme = {
  plain: {color: '#0e1626', backgroundColor: '#ffffff'},
  styles: [
    {types: ['comment', 'prolog', 'doctype', 'cdata'], style: {color: '#4d5b74', fontStyle: 'italic'}},
    {types: ['punctuation', 'operator'], style: {color: '#4d5b74'}},
    {types: ['keyword', 'tag', 'selector', 'atrule', 'important'], style: {color: '#02507f'}},
    {types: ['string', 'char', 'attr-value', 'regex', 'inserted', 'triple-quoted-string', 'url'], style: {color: '#047857'}},
    {types: ['number', 'boolean', 'constant', 'symbol', 'deleted'], style: {color: '#be123c'}},
    {types: ['function', 'class-name', 'decorator', 'annotation'], style: {color: '#4a3aa7'}},
  ],
};

const sdvPrismDark: PrismTheme = {
  plain: {color: '#e9eef6', backgroundColor: '#111b2e'},
  styles: [
    {types: ['comment', 'prolog', 'doctype', 'cdata'], style: {color: '#93a1b8', fontStyle: 'italic'}},
    {types: ['punctuation', 'operator'], style: {color: '#93a1b8'}},
    {types: ['keyword', 'tag', 'selector', 'atrule', 'important'], style: {color: '#4fb6e8'}},
    {types: ['string', 'char', 'attr-value', 'regex', 'inserted', 'triple-quoted-string', 'url'], style: {color: '#10b981'}},
    {types: ['number', 'boolean', 'constant', 'symbol', 'deleted'], style: {color: '#f0537a'}},
    {types: ['function', 'class-name', 'decorator', 'annotation'], style: {color: '#9085e9'}},
  ],
};

// A tutorial's file keeps its number (docs/tutorials/02_cfb_intro.md) but Docusaurus serves it without it
// (/docs/tutorials/cfb_intro), so links to the numbered URL answered 404. Write a static page at each numbered
// URL that sends the reader on; it lists the tutorials at build time, so a new notebook needs no edit here.
function tutorialRedirects() {
  return {
    name: 'tutorial-redirects',
    async postBuild({outDir}: {outDir: string}) {
      for (const file of fs.readdirSync(path.join(__dirname, 'docs/tutorials'))) {
        const m = file.match(/^(\d+_(.+))\.mdx?$/);
        if (!m) continue;
        const to = `/docs/tutorials/${m[2]}`;
        const dir = path.join(outDir, 'docs/tutorials', m[1]);
        fs.mkdirSync(dir, {recursive: true});
        fs.writeFileSync(
          path.join(dir, 'index.html'),
          `<!doctype html><meta charset="utf-8"><title>Moved to ${to}</title><link rel="canonical" href="${to}">` +
            `<meta http-equiv="refresh" content="0; url=${to}"><script>location.replace(${JSON.stringify(to)} + location.hash)</script>` +
            `<p><a href="${to}">${to}</a></p>\n`,
        );
      }
    },
  };
}

const config: Config = {
  // Rspack/SWC build pipeline (@docusaurus/faster). Adopted when the 0.0.72
  // snapshot doubled the built page count (current + one full release tree)
  // and the webpack build started OOM-SIGKILLing the Vercel container.
  future: {
    v4: true,
    faster: true,
  },
  title: 'sdv-py',
  tagline: "The SportsDataverse's Python Package for Sports Data.",
  // The site is served from the CNAME host. With the old subdomain here, every
  // canonical, og:url and og:image pointed off-host, so shares canonicalised and
  // previewed against a domain no link ever used.
  url: 'https://py.sportsdataverse.org',
  baseUrl: '/',
  // The per-league reference subtree under docs/docs/{league}/ is generated from
  // endpoint metadata (`python tools/codegen/generate.py --docs`); the conceptual
  // pages (intro, architecture/, parsers/) are hand-authored. Staying on 'warn'
  // gives a forgiving margin so a single stale cross-link doesn't take the whole
  // site offline; tighten to 'throw' once link coverage is verified clean.
  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'SportsDataverse',
  projectName: 'Sportsdataverse',
  // Docusaurus 3 requires i18n declared explicitly.
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },
  // Detect MDX vs CommonMark per-file. Sphinx-emitted pages stay on
  // CommonMark (`.md`) so MDX 3's stricter parser doesn't trip on
  // bare braces in API signatures; hand-authored MDX files
  // (`.mdx`) keep the full MDX feature set.
  markdown: {
    format: 'detect',
  },
  // Forwards an old #anchor on a split reference page to the family page that now holds it.
  clientModules: [
    require.resolve('./src/clientModules/anchorForward.ts'),
    require.resolve('./src/clientModules/hydrated.ts'),
  ],
  plugins: [
    tutorialRedirects,
    // llms.txt for AI agents (llmstxt.org): an index linking a Markdown copy of every page, the shape
    // pkgdown 2.2 gives the R sites. No llms-full.txt: the docs are ~18 MB, too big to be one useful file.
    [
      'docusaurus-plugin-llms',
      {
        generateLLMsFullTxt: false,
        generateMarkdownFiles: true,
        excludeImports: true,
        removeDuplicateHeadings: true,
      },
    ],
  ],
  scripts: [
    {src: 'https://plausible.io/js/pa-weWpHgIcVfaVUEgwwTBHX.js', async: true},
  ],
  // Plausible's init stub: queues calls until the async script above loads.
  headTags: [
    {
      tagName: 'script',
      attributes: {},
      innerHTML:
        'window.plausible=window.plausible||function(){(plausible.q=plausible.q||[]).push(arguments)},plausible.init=plausible.init||function(i){plausible.o=i||{}};plausible.init()',
    },
    ...fontPreloads,
  ],
  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/sportsdataverse/sportsdataverse-py/edit/main/docs/',
          // Versioning policy: the unversioned tree under docs/docs/ (the codegen-
          // generated reference + hand-authored conceptual pages) is the live
          // DEFAULT served at the root URL (`lastVersion: 'current'`), so it can
          // never drift from the code (the codegen `--check` gate keeps the
          // reference pages == the wrappers). `.github/workflows/docs-deploy.yml`
          // builds the site and publishes it to the `gh-pages` branch, which Vercel
          // serves at py.sportsdataverse.org.
          // It is labelled `main (latest)` — a rolling, collision-proof label — so
          // that the per-release snapshots cut at release time
          // (`yarn version:docs x.y.z`, which freezes a copy under
          // versioned_docs/version-x.y.z) get the exact release numbers without
          // ever clashing with `current`'s label. VERSIONS_TO_KEEP = 3 means only
          // the three newest snapshots are built, so older archives (including the
          // pre-codegen /docs/0.0.50/ tree) are NOT served any more.
          lastVersion: 'current',
          // `current` + the latest 3 release snapshots (see builtVersions above).
          // Auto-derived from versions.json so new releases never re-break the
          // Vercel build by accumulating versioned-docs copies.
          onlyIncludeVersions: builtVersions,
          // A split reference page sits next to its folder of family pages, and the folder's
          // _category_.json links the category to it: drop the page's own sidebar entry so it is listed once.
          async sidebarItemsGenerator({defaultSidebarItemsGenerator, ...args}) {
            const items = await defaultSidebarItemsGenerator(args);
            const listOnce = (list: typeof items): typeof items => {
              const linked = new Set(
                list.flatMap((i) => (i.type === 'category' && i.link?.type === 'doc' ? [i.link.id] : [])),
              );
              return list
                .filter((i) => !(i.type === 'doc' && linked.has(i.id)))
                .map((i) => (i.type === 'category' ? {...i, items: listOnce(i.items)} : i));
            };
            return listOnce(items);
          },
          versions: {
            current: {
              // "main" alone read like a branch name; the label stays unlike any release number.
              label: 'main (latest)',
              path: '',
              banner: 'none',
              // the "Version" chip only on the frozen versions
              badge: false,
            },
          },
        },
        // No blog: release notes live on the CHANGELOG page. Left on, the plugin published an
        // empty /blog and /blog/authors.
        blog: false,
        theme: {
          // The shared family theme first, then what only this site needs.
          customCss: ['./src/css/sdv-theme.css', './src/css/custom.css'],
        },
      } satisfies Preset.Options,
    ],
  ],
  // Offline/local full-text search (no Algolia account, no external crawler —
  // the index is built into the static output at build time). With versioning
  // enabled the plugin indexes only the preferred (`current`/`main`) version,
  // so the index doesn't grow with each release snapshot.
  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        indexBlog: false,
        // Tables are 63.5% of the text on the generated reference pages. Leaving every table out takes
        // the index from 48 MB to 22 MB (8.3 MB to 3.5 MB over the wire). Titles, headings and prose
        // stay searchable. A name that appears only in a table no longer matches: a returned column,
        // a row of the dataset catalog on a loaders page, a row of a tutorial's function table.
        ignoreCssSelectors: ['table'],
        // One index per league directory and one for the package reference, so a league page downloads its
        // own (the largest, MBB, was 1.9 MB raw on the 2026-10-04 prototype) instead of the whole site's 22 MB.
        // Contexts are URL prefixes: a small league cannot share its sport's index, so it gets its own. Pages
        // outside every context (guides, tutorials, the home page) search everything, so a function name typed on
        // the home page still finds its reference page in any league (owner, 2026-10-04); only those pages pay for
        // the site-wide index, and only when the reader starts a search.
        useAllContextsWithNoSearchContext: true,
        searchContextByPaths: [
          ...registry.sports.flatMap((sport) =>
            sport.leagues.map((l) => ({label: l.label, path: `docs/${l.prefix}`})),
          ),
          {label: 'Package reference', path: 'docs/reference'},
        ],
      },
    ],
  ],
  themeConfig: {
    docs: {
      sidebar: {
        hideable: true,
      },
    },
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    image: 'img/Sportsdataverse_gh.png',
    // Social metadata Docusaurus does not emit on its own. og:image alone renders
    // a bare card on several platforms; the type, site name, image dimensions and
    // Twitter attribution are what make the preview complete.
    metadata: [
      { property: 'og:type', content: 'website' },
      { property: 'og:site_name', content: 'sdv-py | SportsDataverse' },
      { property: 'og:image:width', content: '1080' },
      { property: 'og:image:height', content: '540' },
      { property: 'og:image:alt', content: 'SportsDataverse' },
      { name: 'twitter:site', content: '@sportsdataverse' },
      { name: 'twitter:creator', content: '@saiemgilani' },
      { name: 'twitter:image:alt', content: 'SportsDataverse' },
    ],
    navbar: {
      hideOnScroll: true,
      // Navy in both color modes, like sportsdataverse.org; sdv-theme.css sets the colors.
      style: 'dark',
      title: 'sdv-py',
      logo: {
        alt: 'sportsdataverse-py Logo',
        src: 'img/logo.png',
      },
      items: [
        {
          type: 'doc',
          docId: 'intro',
          position: 'left',
          label: 'Docs',
        },
        {
          label: 'News',
          to: 'CHANGELOG',
          position: 'left',
        },
        {
          type: 'docsVersionDropdown',
          position: 'right',
          dropdownActiveClassDisabled: true,
        },
        // SportsDataverse package directory. Sourced from
        //   https://sportsdataverse.org/packages
        //   https://github.com/sportsdataverse/.github/blob/main/profile/README.md
        // Keep this dropdown in sync with those two pages.
        // The `sdv-packages-dropdown` className triggers the multi-column
        // mega-menu layout in sdv-theme.css; without it the 30+ package list
        // overflows the viewport vertically on a typical laptop.
        {
          label: 'SDV',
          position: 'left',
          className: 'sdv-packages-dropdown',
          items: [
            {
              href: 'https://sportsdataverse.org',
              label: 'SportsDataverse',
              target: '_self',
              className: 'sdv-section-header',
            },
            // -- Python --
            {
              label: 'Python Packages',
              href: 'https://py.sportsdataverse.org/',
              target: '_self',
              className: 'sdv-section-header',
            },
            {
              label: 'sportsdataverse-py',
              href: 'https://py.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'sportypy',
              href: 'https://sportypy.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'collegebaseball',
              href: 'https://collegebaseball.readthedocs.io/en/latest/index.html',
              target: '_self',
            },
            {
              label: 'nwslpy',
              href: 'https://github.com/nwslR/nwslpy',
              target: '_self',
            },
            // -- R --
            {
              label: 'R Packages',
              href: 'https://r.sportsdataverse.org/',
              className: 'sdv-section-header',
            },
            {
              label: 'sportsdataverse-R',
              href: 'https://r.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'cfbfastR',
              href: 'https://cfbfastR.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'hoopR',
              href: 'https://hoopR.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'wehoop',
              href: 'https://wehoop.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'fastRhockey',
              href: 'https://fastRhockey.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'baseballr',
              href: 'https://BillPetti.github.io/baseballr/',
              target: '_self',
            },
            {
              label: 'sportyR',
              href: 'https://sportyR.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'ggshakeR',
              href: 'https://abhiamishra.github.io/ggshakeR/',
              target: '_self',
            },
            {
              label: 'soccerAnimate',
              href: 'https://github.com/Dato-Futbol/soccerAnimate',
              target: '_self',
            },
            {
              label: 'oddsapiR',
              href: 'https://oddsapiR.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'mlbplotR',
              href: 'https://camdenk.github.io/mlbplotR/',
              target: '_self',
            },
            {
              label: 'cfbplotR',
              href: 'https://cfbplotR.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'cfb4th',
              href: 'https://cfb4th.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'softballR',
              href: 'https://github.com/sportsdataverse/softballR/',
              target: '_self',
            },
            {
              label: 'nwslR',
              href: 'https://github.com/nwslR/nwslR/',
              target: '_self',
            },
            {
              label: 'usfootballR',
              href: 'https://usfootballR.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'recruitR',
              href: 'https://recruitR.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'puntr',
              href: 'https://puntalytics.github.io/puntr/',
              target: '_self',
            },
            {
              label: 'chessR',
              href: 'https://jaseziv.github.io/chessR/',
              target: '_self',
            },
            // -- Node.js --
            {
              label: 'Node.js Packages',
              href: 'https://js.sportsdataverse.org/',
              className: 'sdv-section-header',
            },
            {
              label: 'sportsdataverse.js',
              href: 'https://js.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'nfl-nerd',
              href: 'https://github.com/nntrn/nfl-nerd/',
              target: '_self',
            },
          ],
        },
        {
          label: 'Data status',
          href: 'https://sportsdataverse.org/status',
          position: 'right',
        },
        {
          label: 'GitHub',
          href: 'https://github.com/sportsdataverse/sportsdataverse-py/',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      // Full columns, as sdvplot's (sub-project 2): the guide, the community, the rest of the SportsDataverse.
      links: [
        {
          title: 'Docs',
          items: [
            {label: 'Getting started', to: '/docs/intro'},
            {label: 'Leagues', to: '/'},
            {label: 'Tutorials', to: '/docs/category/tutorials'},
            {label: 'Changelog', to: '/CHANGELOG'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub', href: 'https://github.com/sportsdataverse/sportsdataverse-py'},
            {label: 'Bluesky', href: 'https://bsky.app/profile/sportsdataverse.org'},
            {label: 'X', href: 'https://twitter.com/sportsdataverse'},
          ],
        },
        {
          title: 'SportsDataverse',
          items: [
            {label: 'sportsdataverse.org', href: 'https://sportsdataverse.org'},
            {label: 'sdvplot', href: 'https://sdvplot.sportsdataverse.org'},
            {label: 'R packages', href: 'https://r.sportsdataverse.org'},
            {label: 'Data status', href: 'https://sportsdataverse.org/status'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} <strong>sportsdataverse-py</strong>, developed by <a href='https://twitter.com/saiemgilani'>Saiem Gilani</a>, part of the <a href='https://sportsdataverse.org'>SportsDataverse</a>.`,
    },
    prism: {
      // The family themes defined above: one surface per mode, every token at 4.5:1 or better.
      theme: sdvPrismLight,
      darkTheme: sdvPrismDark,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
