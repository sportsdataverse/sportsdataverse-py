const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'sportsdataverse-py',
  tagline: "The SportsDataverse's Python Package for Sports Data.",
  url: 'https://sportsdataverse-py.sportsdataverse.org',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/logo.png',
  organizationName: 'SportsDataverse', // Usually your GitHub org/user name.
  projectName: 'sportsdataverse-py', // Usually your repo name.
  themeConfig: {
    hideableSidebar: true,
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    image: 'img/sdv-purple-white-1240.png',
    navbar: {
      hideOnScroll: true,
      title: 'sportsdataverse-py',
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
          label: 'SDV',
          position: 'left',
          items: [
            {
              href: 'https://sportsdataverse.org',
              label: 'SportsDataverse',
              target: '_self',
            },
            {
              label: 'Python Packages',
              to: 'CHANGELOG',
            },
            {
              label: 'sportsdataverse-py',
              href: 'https://py.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'R Packages',
              to: 'CHANGELOG',
            },
            {
              label: 'cfbfastR',
              href: 'https://saiemgilani.github.io/cfbfastR/',
              target: '_self',
            },
            {
              label: 'hoopR',
              href: 'https://hoopR.sportsdataverse.org',
              target: '_self',
            },
            {
              label: 'wehoop',
              href: 'https://wehoop.sportsdataverse.org/',
              target: '_self',
            },
            {
              label: 'recruitR',
              href: 'https://saiemgilani.github.io/recruitR/',
              target: '_self',
            },
            {
              label: 'puntr',
              href: 'https://puntalytics.github.io/puntr/',
              target: '_self',
            },
            {
              label: 'gamezoneR',
              href: 'https://jacklich10.github.io/gamezoneR/',
              target: '_self',
            },
            {
              label: 'cfbplotR',
              href: 'https://kazink36.github.io/cfbplotR/',
              target: '_self',
            },
            {
              label: 'worldfootballR',
              href: 'https://jaseziv.github.io/worldfootballR/',
              target: '_self',
            },
            {
              label: 'baseballr',
              href: 'https://BillPetti.github.io/baseballr/',
              target: '_self',
            },
            {
              label: 'hockeyR',
              href: 'https://hockeyr.netlify.app/',
              target: '_self',
            },
            {
              label: 'fastRhockey',
              href: 'https://BenHowell71.github.io/fastRhockey/',
              target: '_self',
            },
            {
              label: 'Node.js Packages',
              to: 'CHANGELOG',
            },
            {
              label: 'sportsdataverse.js',
              href: 'https://saiemgilani.github.io/sportsdataverse/',
              target: '_self',
            },
            {
              label: 'nfl-nerd',
              href: 'https://github.com/nntrn/nfl-nerd/',
              target: '_self',
            },
          ]
        },
        {
          label: 'GitHub',
          href: 'https://github.com/saiemgilani/sportsdataverse-py/',
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
              href: 'https://twitter.com/saiemgilani',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/saiemgilani/sportsdataverse-py',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} <strong>sportsdataverse-py</strong>, developed by <a href='https://twitter.com/saiemgilani'>Saiem Gilani</a>, part of the <a href='https://sportsdataverse.org'>SportsDataverse</a>.`,
    },
    prism: {
      theme: lightCodeTheme,
      darkTheme: darkCodeTheme,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          editUrl:
            'https://github.com/saiemgilani/sportsdataverse-py/edit/master/docs/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
};