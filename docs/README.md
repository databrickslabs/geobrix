# GeoBrix Documentation

This directory contains the Docusaurus-based documentation for GeoBrix.

## Prerequisites

- Node.js 18.0 or higher
- npm or yarn

## Installation

```bash
cd docs
npm install
```

## Local Development

```bash
npm start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

## Build

```bash
npm run build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

## Deployment

### Using SSH:

```bash
USE_SSH=true npm run deploy
```

### Not using SSH:

```bash
GIT_USER=<Your GitHub username> npm run deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.

## Structure

```
docs/
├── docs/                    # Documentation markdown files
│   ├── intro.md            # Introduction
│   ├── installation.md     # Installation guide
│   ├── quick-start.md      # Quick start guide
│   ├── packages/           # Package documentation
│   ├── readers/            # Reader documentation
│   ├── api/                # API reference
│   └── examples/           # Examples
├── src/                     # Custom React components
│   ├── components/         # Custom components
│   ├── css/               # Custom CSS
│   └── pages/             # Custom pages
├── static/                  # Static assets
│   └── img/               # Images
├── docusaurus.config.js    # Docusaurus configuration
├── sidebars.js             # Sidebar configuration
└── package.json            # Node.js dependencies
```

## Customization

### Configuration

Edit `docusaurus.config.js` to customize:
- Site metadata
- Theme configuration
- Plugin settings
- Navigation

### Sidebar

Edit `sidebars.js` to customize the documentation sidebar structure.

### Styling

Custom styles can be added in `src/css/custom.css`.

### Homepage

The homepage is defined in `src/pages/index.js`.

## Writing Documentation

### Markdown Files

Documentation is written in Markdown with MDX support. Each file should have front matter:

```markdown
---
sidebar_position: 1
---

# Page Title

Content here...
```

### Code Blocks

Use fenced code blocks with language specification:

```markdown
```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)
```
```

### Images

Place images in `static/img/` and reference them:

```markdown
![Alt text](../../../static/img/image.png)
```

## Versioning

To create a new version:

```bash
npm run docusaurus docs:version 1.0.0
```

## Search

Docusaurus comes with built-in search functionality. For production deployments, consider integrating Algolia DocSearch.

## Contributing

1. Make changes to markdown files in `docs/`
2. Test locally with `npm start`
3. Build and verify with `npm run build`
4. Submit pull request

## Troubleshooting

### Build Errors

If you encounter build errors:

1. Clear cache: `npm run clear`
2. Delete `node_modules` and reinstall: `rm -rf node_modules && npm install`
3. Check for broken links or invalid markdown

### Dev Server Issues

If the dev server doesn't start:

1. Check that port 3000 is available
2. Try `npm run clear` and restart
3. Verify Node.js version is 18.0 or higher

## Resources

- [Docusaurus Documentation](https://docusaurus.io/)
- [Markdown Guide](https://www.markdownguide.org/)
- [MDX Documentation](https://mdxjs.com/)

