import React from 'react';
import Link from '@docusaurus/Link';
import styles from './Tier.module.css';

// Surfaces a function exists on; generated into each function section: <Surfaces session uc python scala />
const LABELS = [
  ['session', 'SQL'],
  ['uc', 'Unity Catalog SQL'],
  ['python', 'Python'],
  ['scala', 'Scala'],
];

export default function Surfaces(props) {
  return (
    <Link to="/docs/api/availability" className={styles.tierGroup}>
      {LABELS.filter(([k]) => props[k]).map(([k, label]) => (
        <span key={k} className={`${styles.tier} ${styles[k] || ''}`}>{label}</span>
      ))}
    </Link>
  );
}
