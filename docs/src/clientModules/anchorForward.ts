// A reference page over the size budget keeps its URL as an overview and its functions move to family
// pages (tools/codegen/generate.py, `_split_family_pages`). An old link such as
// /docs/cfb/reference/loaders#load_cfb_pbp then names an anchor the overview no longer has: look it up in
// /anchor-map.json (written by the same codegen run) and go to the family page that holds it.
import type {Location} from 'history';

let anchorMap: Promise<Record<string, Record<string, string>>> | undefined;

export function onRouteDidUpdate({location}: {location: Location}): void {
  if (typeof window === 'undefined' || !location.hash) return;
  let id = location.hash.slice(1);
  try {
    id = decodeURIComponent(id);
  } catch {
    // a malformed escape such as `#%`: keep the raw hash
  }
  if (document.getElementById(id)) return;
  const page = location.pathname.replace(/\/$/, '');
  anchorMap ??= fetch('/anchor-map.json')
    .then((r) => (r.ok ? r.json() : {}))
    .catch(() => ({}));
  void anchorMap.then((map) => {
    // On a family page the map has no entry for the page itself: a function that was added or removed
    // moved the family's pages (`pff-2` -> `pff-3`), so look the anchor up under the overview (the parent path).
    const overview = map[page] ? page : page.slice(0, page.lastIndexOf('/'));
    const anchors = map[overview];
    // `<fn>-returns` and `<fn>-example` (the per-function sub-headings) live on the function's page too
    const slug = anchors && (anchors[id] ?? anchors[id.replace(/-(returns|example)$/, '')]);
    if (slug && `${overview}/${slug}` !== page) {
      window.location.replace(`${overview}/${slug}${location.search}#${encodeURIComponent(id)}`);
    }
  });
}
