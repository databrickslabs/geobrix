/**
 * Re-scroll to the URL hash after the page has settled.
 *
 * Long reference pages (e.g. api/raster-functions) are tall enough that the
 * browser's initial hash scroll fires before content above the target finishes
 * sizing (fonts, code blocks, images), landing short of the anchor and never
 * re-correcting. This module re-runs `scrollIntoView` for the current hash once
 * fonts are ready and again shortly after, so a deep anchor lands on its section
 * on direct load and cross-page navigation. Same-page hash changes (in-page
 * clicks, scroll-spy) are left to Docusaurus so we never fight an active scroll.
 *
 * `scrollIntoView` respects the `scroll-margin-top` Docusaurus sets on headings,
 * so the target lands just below the fixed navbar.
 */

function reScrollToHash() {
  if (typeof window === 'undefined' || typeof document === 'undefined') return;
  const {hash} = window.location;
  if (!hash || hash.length < 2) return;
  const id = decodeURIComponent(hash.slice(1));

  const scroll = () => {
    const el = document.getElementById(id);
    if (el) el.scrollIntoView();
  };
  const runSeq = () => {
    scroll();
    // re-correct after late layout shifts (images/code finishing paint)
    window.setTimeout(scroll, 300);
    window.setTimeout(scroll, 900);
  };

  if (document.fonts && document.fonts.ready) {
    document.fonts.ready.then(runSeq).catch(runSeq);
  } else {
    runSeq();
  }
}

// Initial (direct / hard) load.
if (typeof window !== 'undefined') {
  if (document.readyState === 'complete') {
    reScrollToHash();
  } else {
    window.addEventListener('load', reScrollToHash, {once: true});
  }
}

// Cross-page client-side navigation that targets a hash. The initial render is
// handled above (previousLocation is null then), and same-page hash-only changes
// are intentionally ignored so we don't interrupt the user's own scrolling.
export function onRouteDidUpdate({location, previousLocation}) {
  if (!previousLocation) return;
  if (location.hash && previousLocation.pathname !== location.pathname) {
    reScrollToHash();
  }
}
