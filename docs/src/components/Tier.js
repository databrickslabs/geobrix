import React from 'react';
import Link from '@docusaurus/Link';
import styles from './Tier.module.css';

// Availability badge for Execution Tiers. Usage in MDX:
//   <Tier both/>  <Tier light/>  <Tier heavy/>
//
// Lightweight impl-technique tags (link to the performance page). Usage in MDX:
//   <Impl udtf/>          — Streaming UDTF
//   <Impl groupedAgg/>    — Grouped-agg UDF
export default function Tier({both, light, heavy}) {
  const pills = [];
  if (both || light) pills.push(['light', 'Lightweight']);
  if (both || heavy) pills.push(['heavy', 'Heavyweight']);
  return (
    <span className={styles.tierGroup}>
      {pills.map(([k, label]) => (
        <span key={k} className={`${styles.tier} ${styles[k]}`}>{label}</span>
      ))}
    </span>
  );
}

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
