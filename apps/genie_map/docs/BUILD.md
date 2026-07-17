# Build notes

## Baseline

This app was seeded from the kepler.gl + Databricks AppKit demo prototype
(`kepler-demo`), copied into `apps/genie_map/` unmodified. Before any changes,
the copied source was verified to build cleanly so that later build failures are
attributable to our changes rather than the starting point.

### Environment built against

- Node.js v25.2.1 (satisfies the `engines.node >= 20` floor)
- pnpm 10.34.4 (lockfile pinned to pnpm 10.30.3; same major, compatible)
- TypeScript 5.9.3, Vite 6.4.1

### Install

```
pnpm install --frozen-lockfile
```

The frozen lockfile install completed successfully with no resolution changes.
pnpm reports "Ignored build scripts" for a handful of dependencies (esbuild,
protobufjs, AppKit, deckgl typings, heroui shared-utils); this is pnpm's default
lifecycle-script sandboxing and does not affect the build.

### Build

```
pnpm build
```

This runs the server type-check/emit (`tsc -p tsconfig.server.json`) followed by
the client bundle (`vite build`). Both stages complete successfully:

- `dist/server/index.js` is produced.
- `dist/client/` is produced (`index.html` plus the `assets/` bundle).

Vite prints a chunk-size advisory for the large client bundle; this is an
informational warning, not a build error.

### Environment variables

The build is green with no `.env` present. `VITE_DATASET_TABLE` and related
runtime settings are only required at query time, not at build time, so a
missing environment file does not affect this baseline.
