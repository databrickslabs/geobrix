import React from 'react';
import Link from '@docusaurus/Link';
import styles from './ZarrNav.module.css';

// "Zarr in GeoBrix": the one orientation block shown under the title of every Zarr page.
// Usage: <ZarrNav current="writer" />. The current page shows in bold and is not a link.
// Add or rename a Zarr page here and every page picks it up.
const ROWS = [
  {
    label: 'Reference',
    items: [['functions', 'Function reference', '/docs/api/zarr-functions']],
  },
  {
    label: 'Concepts',
    items: [
      ['stores', 'Stores', '/docs/api/zarr-stores'],
      ['registered', 'Registering and syncing', '/docs/api/zarr-registered-stores'],
      ['transactions', 'Transactions', '/docs/api/zarr-transactions'],
      ['versions', 'Versions and tags', '/docs/api/zarr-versions'],
      ['lifecycle', 'Lifecycle and retention', '/docs/api/zarr-lifecycle'],
      ['performance', 'Performance and limits', '/docs/api/zarr-performance'],
    ],
  },
  {
    label: 'How-to',
    items: [
      ['reader', 'Reader', '/docs/readers/zarr'],
      ['writer', 'Writer', '/docs/writers/zarr'],
    ],
  },
  {
    label: 'Example',
    items: [['notebook', 'Weather features notebook', '/docs/notebooks/zarr-weather-features']],
  },
];

export default function ZarrNav({current}) {
  return (
    <nav className={styles.nav} aria-label="Zarr in GeoBrix">
      <div className={styles.title}>Zarr in GeoBrix</div>
      {ROWS.map(({label, items}) => (
        <div className={styles.row} key={label}>
          <span className={styles.label}>{label}</span>
          {items.map(([key, text, to], i) => (
            <React.Fragment key={key}>
              {i > 0 && <span className={styles.sep}>·</span>}
              {key === current ? (
                <span className={styles.here} aria-current="page">{text}</span>
              ) : (
                <Link to={to}>{text}</Link>
              )}
            </React.Fragment>
          ))}
        </div>
      ))}
    </nav>
  );
}
