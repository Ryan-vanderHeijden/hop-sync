// A very small stand-in for the browser DOM, just enough to run pane.js and
// jobs.js under `node --test` and catch typos. It is NOT a real browser.

class ClassList {
  constructor(owner) { this.owner = owner; }
  names() { return this.owner.className.split(/\s+/).filter(Boolean); }
  set(names) { this.owner.className = names.join(" "); }
  add(...n) { this.set([...new Set([...this.names(), ...n])]); }
  remove(...n) { this.set(this.names().filter((x) => !n.includes(x))); }
  contains(n) { return this.names().includes(n); }
  toggle(n, force) {
    const on = force === undefined ? !this.contains(n) : force;
    if (on) this.add(n); else this.remove(n);
    return on;
  }
}

// Supports ".a", ".a.b", ".a:not(.b)" and "tag".
function matches(node, selector) {
  const not = selector.match(/:not\(\.([\w-]+)\)/);
  const base = selector.replace(/:not\(.*\)/, "");
  if (not && node.classList.contains(not[1])) return false;
  return base.split(".").every((part, i) => (i === 0 && part === "" ) || (i === 0 ? node.tagName === part.toUpperCase() : node.classList.contains(part)));
}

export class FakeElement {
  constructor(tag) {
    this.tagName = tag.toUpperCase();
    this.className = "";
    this.classList = new ClassList(this);
    this.children = [];
    this.parent = null;
    this.dataset = {};
    this.style = {};
    this.attributes = {};
    this.listeners = {};
    this.ownText = "";
    this.value = "";
    this.hidden = false;
  }
  set textContent(text) { this.children = []; this.ownText = String(text); }
  get textContent() { return this.ownText + this.children.map((c) => c.textContent).join(""); }
  append(...nodes) { for (const n of nodes) { n.parent = this; this.children.push(n); } }
  replaceChildren(...nodes) { this.children = []; this.ownText = ""; this.append(...nodes); }
  setAttribute(name, value) { this.attributes[name] = value; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  dispatch(type, event = {}) {
    event.target ||= this;
    event.preventDefault ||= () => {};
    let node = this;
    while (node) { for (const fn of node.listeners[type] || []) fn(event); node = node.parent; }
  }
  all() { return this.children.flatMap((c) => [c, ...c.all()]); }
  querySelectorAll(selector) { return this.all().filter((n) => selector.split(",").some((s) => matches(n, s.trim()))); }
  closest(selector) { let n = this; while (n) { if (matches(n, selector)) return n; n = n.parent; } return null; }
  contains(other) { let n = other; while (n) { if (n === this) return true; n = n.parent; } return false; }
  focus() { globalThis.document.activeElement = this; }
  scrollIntoView() {}
  showModal() { this.open = true; }
  close() { if (this.open) { this.open = false; this.dispatch("close"); } }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((c) => c !== this); }
}

export function installFakeDom() {
  const document = {
    activeElement: null,
    createElement: (tag) => new FakeElement(tag),
    getElementById: () => null,
    querySelectorAll: () => [],
    addEventListener: () => {},
  };
  globalThis.document = document;
  globalThis.requestAnimationFrame = (fn) => fn();
  globalThis.setInterval = () => 0;
  globalThis.localStorage = { store: {}, getItem(k) { return this.store[k] ?? null; }, setItem(k, v) { this.store[k] = v; } };
  globalThis.location = { protocol: "http:", host: "127.0.0.1:1" };
  return document;
}
