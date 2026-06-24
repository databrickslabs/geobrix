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
      label: 'Notebooks',
      collapsed: true,
      items: [
        'notebooks/eo-series',
        'notebooks/xview',
        'notebooks/h3-rasterize',
      ],
    },
    {
      type: 'category',
      label: 'Sample Data',
      collapsed: true,
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
      label: 'Readers & Writers',
      collapsed: true,
      items: [
        {
          type: 'category',
          label: 'Readers',
          collapsed: true,
          items: [
            'readers/overview',
            { type: 'category', label: 'General', collapsed: true, items: ['readers/raster', 'readers/vector'] },
            { type: 'category', label: 'Named', collapsed: true, items: ['readers/geotiff', 'readers/shapefile', 'readers/geojson', 'readers/geopackage', 'readers/filegdb'] },
          ],
        },
        {
          type: 'category',
          label: 'Writers',
          collapsed: true,
          items: [
            'writers/overview',
            { type: 'category', label: 'General', collapsed: true, items: ['writers/raster', 'writers/vector'] },
            { type: 'category', label: 'Named', collapsed: true, items: ['writers/geotiff', 'writers/pmtiles', 'writers/shapefile', 'writers/geojson', 'writers/geojsonl', 'writers/geopackage', 'writers/filegdb'] },
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Functions',
      collapsed: true,
      items: [
        'api/overview',
        'api/tile-structure',
        'api/execution-tiers',
        'api/language-bindings',
        {
          type: 'category',
          label: 'RasterX',
          collapsed: true,
          link: { type: 'doc', id: 'api/raster-functions' },
          items: [
            'api/h3-raster-tessellation',
          ],
        },
        { type: 'doc', id: 'api/vectorx-functions', label: 'VectorX' },
        { type: 'doc', id: 'api/gridx-functions', label: 'GridX' },
        { type: 'doc', id: 'api/vizx', label: 'VizX' },
        { type: 'doc', id: 'api/pmtiles-functions', label: 'PMTiles' },
        { type: 'doc', id: 'api/stac', label: 'STAC' },
        {
          type: 'category',
          label: 'Performance & Benchmarking',
          collapsed: true,
          items: [
            'api/performance',
            'api/benchmarking',
          ],
        },
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
      collapsed: true,
      items: [
        'advanced/overview',
        'advanced/custom-udfs',
        'advanced/gdal-cli',
        'advanced/library-integration',
      ],
    },
    'developers',
    'security',
    'limitations',
    'support',
  ],
};

export default sidebars;

