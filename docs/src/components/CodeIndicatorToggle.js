import React from 'react';
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';

/**
 * Toggle component to show/hide code indicators. Default: off (user opts in to show).
 * Uses showCodeIndicators so "default off" is unambiguous (no legacy key).
 */
export default function CodeIndicatorToggle() {
  const [show, setShow] = React.useState(false);
  const [isMounted, setIsMounted] = React.useState(false);

  React.useEffect(() => {
    setIsMounted(true);
    if (ExecutionEnvironment.canUseDOM) {
      const visible = localStorage.getItem('showCodeIndicators') === 'true';
      setShow(visible);
    }
  }, []);

  const handleToggle = () => {
    const newShow = !show;
    setShow(newShow);
    if (ExecutionEnvironment.canUseDOM) {
      localStorage.setItem('showCodeIndicators', newShow.toString());
      window.dispatchEvent(new Event('storage'));
      window.location.reload();
    }
  };

  // Don't render on server-side
  if (!isMounted) {
    return null;
  }

  return (
    <div style={{
      position: 'fixed',
      bottom: '20px',
      right: '20px',
      zIndex: 1000,
      backgroundColor: 'white',
      borderRadius: '8px',
      boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
      padding: '12px 16px',
      display: 'flex',
      alignItems: 'center',
      gap: '10px',
      fontSize: '14px',
      cursor: 'pointer',
      border: '1px solid #ddd'
    }} onClick={handleToggle}>
      <input
        type="checkbox"
        checked={show}
        onChange={handleToggle}
        style={{ cursor: 'pointer' }}
      />
      <span style={{ userSelect: 'none' }}>
        {show ? '✅' : '📄🔗'} Code indicators
      </span>
    </div>
  );
}
