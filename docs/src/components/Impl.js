import React from 'react';
import Link from '@docusaurus/Link';
import styles from './Tier.module.css';

// Implementation-technique tags (link to the performance page). Usage in MDX:
//   <Impl udtf/>          -- Streaming UDTF
//   <Impl groupedAgg/>    -- Grouped-agg UDF
export function Impl({udtf, groupedAgg}) {
  if (udtf) {
    return (
      <Link to="/docs/api/performance#streaming-udtfs" className={styles.implLink}>
        <span className={`${styles.tier} ${styles.implUdtf}`}>Streaming UDTF</span>
      </Link>
    );
  }
  if (groupedAgg) {
    return (
      <Link to="/docs/api/performance#grouped-aggregate-udfs" className={styles.implLink}>
        <span className={`${styles.tier} ${styles.implAgg}`}>Grouped-agg UDF</span>
      </Link>
    );
  }
  return null;
}

export default Impl;
