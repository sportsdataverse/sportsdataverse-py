import * as fs from 'fs';
import * as path from 'path';
import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

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
  url: 'https://sportsdataverse-py.sportsdataverse.org',
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
  scripts: [
    {
      src: 'https://plausible.io/js/script.js',
      defer: true,
      'data-domain': 'py.sportsdataverse.org',
    },
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
          // DEFAULT served at the root URL (`lastVersion: 'current'`), so every
          // push republishes it via Vercel and it can never drift from the code
          // (the codegen `--check` gate keeps the reference pages == the wrappers).
          // It is labelled `main` — a rolling, collision-proof label — so that the
          // per-release snapshots cut at release time (`yarn version:docs x.y.z`,
          // which freezes a copy under versioned_docs/version-x.y.z) get the exact
          // release numbers without ever clashing with `current`'s label. The
          // legacy pre-codegen Sphinx docs stay archived at /docs/0.0.50/.
          lastVersion: 'current',
          // `current` + the latest 3 release snapshots (see builtVersions above).
          // Auto-derived from versions.json so new releases never re-break the
          // Vercel build by accumulating versioned-docs copies.
          onlyIncludeVersions: builtVersions,
          versions: {
            current: {
              label: 'main',
              path: '',
              banner: 'none',
            },
          },
        },
        theme: {
          customCss: './src/css/custom.css',
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
    navbar: {
      hideOnScroll: true,
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
        // mega-menu layout in custom.css; without it the 30+ package list
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
          label: 'GitHub',
          href: 'https://github.com/sportsdataverse/sportsdataverse-py/',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Docs',
              to: '/docs/intro',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Twitter (Author)',
              href: 'https://twitter.com/saiemgilani',
            },
            {
              label: 'Twitter (SportsDataverse)',
              href: 'https://twitter.com/sportsdataverse',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/sportsdataverse/sportsdataverse-py',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} <strong>sportsdataverse-py</strong>, developed by <a href='https://twitter.com/saiemgilani'>Saiem Gilani</a>, part of the <a href='https://sportsdataverse.org'>SportsDataverse</a>.`,
    },
    prism: {
      // Light mode: GitHub Light (clean light-on-white, matches the docs surface).
      // Dark mode: okaidia — prism-react-renderer's Monokai port (bg #272822,
      // green strings #a6e22e, pink keywords #f92672). Replaces dracula.
      theme: prismThemes.github,
      darkTheme: prismThemes.okaidia,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
