// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Everything you need to know about Airbyte!',
  tagline: 'Data integration made simple, secure and extensible.',
  url: 'https://docs.airbyte.com',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  // missing asset
  // favicon: 'img/favicon.ico',
  organizationName: 'airbyte', // GitHub org/user name.
  projectName: 'airbyte', // Repo name.

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/airbytehq/airbyte/blob/gitbook/v1/README.md',
          path: '../docs'
        },
        blog: {
          showReadingTime: true,
          editUrl:
            'https://github.com/airbytehq/airbyte/blob/gitbook/v1/README.md',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Airbyte Documentation',
        logo: {
          alt: 'simple, secure, extensible',
          src: 'img/logo.svg',
        },
        items: [
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Tutorial',
                to: '/docs/intro',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Github',
                href: 'https://github.com/airbytehq/airbyte',
              },
              {
                label: 'Slack',
                href: 'https://slack.airbyte.io/',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/airbytehq',
              },
              {
                label: 'LinkedIn',
                href: 'https://www.linkedin.com/company/airbytehq/',
              },              
              {
                label: 'Facebook',
                href: 'https://www.facebook.com/AirbyteHQ',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/facebook/docusaurus',
              },
            ],
          },
        ],
        copyright: `© ${new Date().getFullYear()} Airbyte, Inc.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
