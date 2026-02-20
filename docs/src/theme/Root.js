import React from 'react';
import CodeIndicatorToggle from '../components/CodeIndicatorToggle';

/**
 * Root wrapper for Docusaurus - adds code indicator toggle to all pages
 */
export default function Root({children}) {
  return (
    <>
      {children}
      <CodeIndicatorToggle />
    </>
  );
}
