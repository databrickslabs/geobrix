import React from 'react';
import Link from '@docusaurus/Link';
import styles from './Tier.module.css';

// Surfaces a function exists on; generated into each function section: <Surfaces session uc python scala jar="gbx_x" />
// The `uc` prop comes from the function manifest (a function with a Unity Catalog level), so the pointer to
// the Unity Catalog page is generated, not hand-listed.
// Scala is the performance tier (the JAR), not an API: its tag links to the "Functions the JAR speeds up" page.
// The generated `jar` prop is the anchor of the function's row there; a Scala form the default never picks has
// no row, so its tag links to the page itself.
const LABELS = [
  ['session', 'SQL'],
  ['uc', 'Unity Catalog SQL'],
  ['python', 'Python'],
];

export default function Surfaces(props) {
  const jarTo = '/docs/api/jar-functions' + (props.jar ? `#${props.jar}` : '');
  return (
    <>
      <span className={styles.tierGroup}>
        <Link to="/docs/api/availability" className={`${styles.tierGroup} ${styles.implLink}`}>
          {LABELS.filter(([k]) => props[k]).map(([k, label]) => (
            <span key={k} className={`${styles.tier} ${styles[k] || ''}`}>{label}</span>
          ))}
        </Link>
        {props.scala && (
          <Link
            to={jarTo}
            className={styles.implLink}
            title="Functions the JAR speeds up"
            data-gbx-scala-link="true"
          >
            <span className={`${styles.tier} ${styles.scala}`}>Scala</span>
          </Link>
        )}
      </span>
      {props.uc && (
        <div className={styles.ucNote} data-gbx-uc-pointer="true">
          Also available as a Unity Catalog function for SQL warehouses.{' '}
          <Link to="/docs/api/uc-functions">See Unity Catalog functions</Link>.
        </div>
      )}
    </>
  );
}
