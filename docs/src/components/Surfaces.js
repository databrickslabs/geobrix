import React from 'react';
import Link from '@docusaurus/Link';
import styles from './Tier.module.css';

// Surfaces a function exists on; generated into each function section: <Surfaces session uc python scala />
// The `uc` prop comes from the function manifest (a function with a Unity Catalog level), so the pointer to
// the Unity Catalog page is generated, not hand-listed.
const LABELS = [
  ['session', 'SQL'],
  ['uc', 'Unity Catalog SQL'],
  ['python', 'Python'],
  ['scala', 'Scala'],
];

export default function Surfaces(props) {
  return (
    <>
      <Link to="/docs/api/availability" className={styles.tierGroup}>
        {LABELS.filter(([k]) => props[k]).map(([k, label]) => (
          <span key={k} className={`${styles.tier} ${styles[k] || ''}`}>{label}</span>
        ))}
      </Link>
      {props.uc && (
        <div className={styles.ucNote} data-gbx-uc-pointer="true">
          Also available as a Unity Catalog function for SQL warehouses.{' '}
          <Link to="/docs/api/uc-functions">See Unity Catalog functions</Link>.
        </div>
      )}
    </>
  );
}
