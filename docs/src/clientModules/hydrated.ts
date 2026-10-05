// Marks the page once its code blocks show the reader's color mode. The server renders every code block in the
// light Prism theme (inline colors) and React swaps in the dark one in a render after hydration, seconds later on
// a long page (finding 11), so custom.css hides code blocks in dark mode until this attribute appears (or for at
// most 6 s). The swap may replace the elements, so check the first block's surface on each frame until it is dark.
export function onRouteDidUpdate(): void {
  if (typeof window === 'undefined') return;
  const root = document.documentElement;
  if (root.dataset.hydrated) return;
  let frames = 0;
  const check = () => {
    const pre = document.querySelector('.theme-code-block pre');
    const [r, g, b] = pre ? (getComputedStyle(pre).backgroundColor.match(/\d+/g) ?? []).map(Number) : [0, 0, 0];
    // light mode, no code block, or the dark surface is in: show the blocks (and stop after about 10 s)
    if (root.dataset.theme !== 'dark' || r + g + b < 384 || ++frames > 600) root.dataset.hydrated = 'true';
    else requestAnimationFrame(check);
  };
  check();
}
