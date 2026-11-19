// Tiny helper to build elements. File names and messages come from remote
// machines, so they only ever go in through textContent (never parsed as HTML).

export function el(tag, className = "", text = "") {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text) node.textContent = text;
  return node;
}

export function clear(node) {
  node.replaceChildren();
}
