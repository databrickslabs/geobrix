/**
 * Send an old link into the long "Zarr Stores" page (/docs/formats/zarr#section) to the topic
 * page that now holds the section. The client redirect for /docs/formats/zarr lands on
 * /docs/api/zarr-stores and keeps the #anchor; this module moves the anchors that live on
 * other pages. An anchor not listed here stays where it landed.
 */
const MOVED = {
  'register-a-store-in-place': 'zarr-registered-stores#register-a-store-in-place',
  'outside-changes-drift-sync-and-publish': 'zarr-registered-stores#outside-changes-drift-sync-and-publish',
  'one-owner-per-root': 'zarr-registered-stores#one-owner-per-root',
  'transactions': 'zarr-transactions',
  'concurrent-writers': 'zarr-transactions#concurrent-writers',
  'snapshots-time-travel-and-tags': 'zarr-versions',
  'lifecycle-and-retention': 'zarr-lifecycle',
  'chunk-rows-and-shard-rows': 'zarr-lifecycle#chunk-rows-and-shard-rows',
  'performance': 'zarr-performance',
  'reads': 'zarr-performance#reads',
  'writes': 'zarr-performance#writes',
  'limits': 'zarr-performance#limits',
};

export function onRouteDidUpdate({location}) {
  if (typeof window === 'undefined') return;
  const m = location.pathname.match(/^(.*\/docs\/api\/)zarr-stores\/?$/);
  if (!m || !location.hash) return;
  const target = MOVED[decodeURIComponent(location.hash.slice(1))];
  if (target) window.location.replace(m[1] + target);
}
