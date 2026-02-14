/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  // By default, Docusaurus generates a sidebar from the docs folder structure
  tutorialSidebar: [
    'intro',
    'installation',
    'quick-start',
    'databricks-spatial',
    'beta-release-notes',
    {
      type: 'category',
      label: 'Sample Data',
      items: [
        'sample-data/overview',
        'sample-data/setup',
        'sample-data/vector-data',
        'sample-data/raster-data',
        'sample-data/additional',
      ],
    },
    {
      type: 'category',
      label: 'Packages',
      items: [
        'packages/overview',
        'packages/rasterx',
        'packages/gridx',
        'packages/vectorx',
      ],
    },
    {
      type: 'category',
      label: 'Readers',
      items: [
        'readers/overview',
        'readers/gdal',
        'readers/gtiff',
        'readers/ogr',
        'readers/shapefile',
        'readers/geojson',
        'readers/geopackage',
        'readers/filegdb',
      ],
    },
    {
      type: 'category',
      label: 'API Reference',
      items: [
        'api/overview',
        'api/tile-structure',
        {
          type: 'category',
          label: 'Function Reference',
          items: [
            'api/rasterx-functions',
            'api/gridx-functions',
            'api/vectorx-functions',
          ],
        },
        'api/scala',
        'api/python',
        'api/sql',
      ],
    },
    // Temporarily hidden until Examples section is ready to ship
    // {
    //   type: 'category',
    //   label: 'Examples',
    //   items: [
    //     'examples/overview',
    //   ],
    // },
    {
      type: 'category',
      label: 'Advanced Usage',
      items: [
        'advanced/overview',
        'advanced/custom-udfs',
        'advanced/gdal-cli',
        'advanced/library-integration',
      ],
    },
    'developers',
    'limitations',
    'support',
  ],
};

export default sidebars;

