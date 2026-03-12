// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'GeoBrix',
  tagline: 'High-performance spatial processing library for Databricks',
  favicon: 'img/favicon.ico',

  // GitHub Pages project site: https://databrickslabs.github.io/geobrix/
  // Set DOCS_STATIC_ZIP=1 for relative paths (static zip). Set DOCS_PRIVATE_PAGES=1 for legacy private Pages URL.
  url: process.env.DOCS_PRIVATE_PAGES === '1' ? 'https://friendly-fiesta-2eo74ww.pages.github.io' : 'https://databrickslabs.github.io',
  baseUrl: process.env.DOCS_STATIC_ZIP === '1' ? './' : (process.env.DOCS_PRIVATE_PAGES === '1' ? '/' : '/geobrix/'),
  // Hash router for static zip so file:// opens work (pathname is then in hash, e.g. #/)
  ...(process.env.DOCS_STATIC_ZIP === '1' ? { future: { experimental_router: 'hash' } } : {}),

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'databrickslabs', // Usually your GitHub org/user name.
  projectName: 'geobrix', // Usually your repo name.

  onBrokenLinks: 'warn',

  // Markdown configuration
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  // Custom webpack plugin to enable importing test files
  plugins: [
    function customWebpackPlugin(context, options) {
      return {
        name: 'custom-webpack-plugin',
        configureWebpack(config, isServer, utils) {
          return {
            module: {
              rules: [
                {
                  test: /\.(py|scala|snippet)$/,
                  use: 'raw-loader',
                },
                {
                  test: /\.ipynb$/,
                  use: 'raw-loader',
                },
              ],
            },
          };
        },
      };
    },
    // Convert absolute paths to relative in HTML/CSS so static zip works when opening index.html from any folder (file://)
    ...(process.env.DOCS_STATIC_ZIP === '1' ? ['@someok/docusaurus-plugin-relative-paths'] : []),
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/databrickslabs/geobrix/tree/main/docs/',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/databrickslabs/geobrix/tree/main/docs/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'img/geobrix-social-card.jpg',
      navbar: {
        title: 'GeoBrix',
        logo: {
          alt: 'GeoBrix Logo',
          src: 'img/logo.png',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: 'Documentation',
          },
          {
            to: '/docs/api/overview',
            label: 'API Reference',
            position: 'left'
          },
          {
            href: 'https://github.com/databrickslabs/geobrix',
            label: 'GitHub',
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
                label: 'Getting Started',
                to: '/docs/intro',
              },
              {
                label: 'Installation',
                to: '/docs/installation',
              },
              {
                label: 'Quick Start',
                to: '/docs/quick-start',
              },
            ],
          },
          {
            title: 'Packages',
            items: [
              {
                label: 'RasterX',
                to: '/docs/packages/rasterx',
              },
              {
                label: 'GridX',
                to: '/docs/packages/gridx',
              },
              {
                label: 'VectorX',
                to: '/docs/packages/vectorx',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/databrickslabs/geobrix',
              },
              {
                label: 'Databricks Labs',
                href: 'https://www.databricks.com/learn/labs',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Databricks Labs. Built with Docusaurus.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['java', 'bash', 'sql'],
      },
    }),
};

export default config;

