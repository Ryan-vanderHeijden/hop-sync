// One file-browser pane: host picker, path bar, file list, selection, drag source/target.

import { listDir } from "./api.js";
import { clear, el } from "./dom.js";
import { formatMtime, humanSize, posixDirname, posixJoin } from "./format.js";

export const DRAG_TYPE = "application/x-hopsync";

// The pane a drag started in (the drag data itself cannot be read until drop).
let dragSource = null;

export class Pane {
  // callbacks: { onLoaded(pane), onDrop(payload, pane, targetDirectory) }
  constructor(root, side, callbacks) {
    this.root = root;
    this.side = side;
    this.callbacks = callbacks;
    this.host = "";
    this.path = "";
    this.entries = [];
    this.selected = new Set(); // indexes into this.entries
    this.anchor = null; // where shift+click ranges start
    this.cursor = null; // the row the arrow keys move
    this.requestId = 0;
    this.controller = null;
    this.hosts = [];
    this.buildDom();
    this.attachEvents();
  }

  // ---- building the DOM --------------------------------------------------

  buildDom() {
    this.root.classList.add("pane");
    const bar = el("div", "pane-bar");
    this.hostSelect = el("select", "host-select");
    this.hostSelect.setAttribute("aria-label", `Host (${this.side} pane)`);
    this.dot = el("span", "dot");
    this.upButton = el("button", "", "Up");
    this.pathInput = el("input", "path-input");
    this.pathInput.setAttribute("aria-label", `Path (${this.side} pane)`);
    this.pathInput.spellcheck = false;
    this.refreshButton = el("button", "", "Refresh");
    bar.append(this.hostSelect, this.dot, this.upButton, this.pathInput, this.refreshButton);

    this.list = el("div", "list");
    this.list.tabIndex = 0;
    this.status = el("div", "status");
    this.root.append(bar, this.list, this.status);
  }

  attachEvents() {
    this.hostSelect.addEventListener("change", () => this.load(this.hostSelect.value, ""));
    this.upButton.addEventListener("click", () => this.up());
    this.refreshButton.addEventListener("click", () => this.refresh());
    this.pathInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") this.load(this.host, this.pathInput.value.trim());
    });
    this.list.addEventListener("click", (event) => this.onClick(event));
    this.list.addEventListener("dblclick", (event) => this.onDoubleClick(event));
    this.list.addEventListener("keydown", (event) => this.onKeyDown(event));
    this.list.addEventListener("dragstart", (event) => this.onDragStart(event));
    this.root.addEventListener("dragend", () => this.onDragEnd());
    this.root.addEventListener("dragover", (event) => this.onDragOver(event));
    this.root.addEventListener("dragleave", (event) => this.onDragLeave(event));
    this.root.addEventListener("drop", (event) => this.onDrop(event));
  }

  // ---- hosts ---------------------------------------------------------------

  setHosts(hosts) {
    this.hosts = hosts;
    const previous = this.host;
    clear(this.hostSelect);
    for (const info of hosts) {
      const option = el("option", "", info.alias);
      option.value = info.alias;
      this.hostSelect.append(option);
    }
    this.hostSelect.value = previous || (hosts[0] ? hosts[0].alias : "");
    this.updateDot();
  }

  updateDot() {
    const info = this.hosts.find((h) => h.alias === this.host);
    let state = "unknown";
    if (info && info.error_kind) state = "error";
    else if (info && info.connected) state = "connected";
    this.dot.className = `dot ${state}`;
    this.dot.title = state === "error" ? "login problem" : state;
  }

  // ---- loading a directory -------------------------------------------------

  // Lists ONLY when asked to. A newer request cancels the older one.
  async load(host, path) {
    if (this.controller) this.controller.abort();
    this.controller = new AbortController();
    const myRequest = ++this.requestId;
    this.host = host;
    this.hostSelect.value = host;
    this.updateDot();
    this.showStatus("Loading…");
    clear(this.list);
    try {
      const result = await listDir(host, path, this.controller.signal);
      if (myRequest !== this.requestId) return; // a newer request took over
      this.showListing(result);
    } catch (error) {
      if (error.name === "AbortError" || myRequest !== this.requestId) return;
      this.showError(error.message);
    }
  }

  showListing(result) {
    this.path = result.abs_path;
    this.entries = result.entries;
    this.selected = new Set();
    this.anchor = this.cursor = null;
    this.pathInput.value = this.path;
    this.showStatus(`${this.entries.length} items`);
    this.render();
    this.remember();
    this.callbacks.onLoaded(this);
  }

  showError(message) {
    this.entries = [];
    this.selected = new Set();
    this.pathInput.value = this.path;
    clear(this.list);
    this.status.className = "status error";
    this.status.textContent = message;
    this.callbacks.onLoaded(this);
  }

  showStatus(text) {
    this.status.className = "status";
    this.status.textContent = text;
  }

  refresh() {
    return this.load(this.host, this.path);
  }

  up() {
    if (this.path) this.load(this.host, posixDirname(this.path));
  }

  // ---- remembering the last place (a per-browser convenience only) ----------

  remember() {
    try {
      localStorage.setItem(`hopsync.pane.${this.side}`, JSON.stringify({ host: this.host, path: this.path }));
    } catch {
      // storage can be blocked; nothing depends on it
    }
  }

  static recall(side) {
    try {
      return JSON.parse(localStorage.getItem(`hopsync.pane.${side}`)) || {};
    } catch {
      return {};
    }
  }

  // ---- drawing the list ----------------------------------------------------

  render() {
    clear(this.list);
    const head = el("div", "row head");
    head.append(el("span", "marker"), el("span", "name", "Name"), el("span", "size", "Size"), el("span", "mtime", "Modified"));
    this.list.append(head);
    this.entries.forEach((entry, index) => this.list.append(this.makeRow(entry, index)));
    this.paintSelection();
  }

  makeRow(entry, index) {
    const kind = entry.is_dir ? "dir" : entry.is_link ? "link" : "file";
    const row = el("div", `row ${kind}`);
    row.draggable = true;
    row.dataset.index = String(index);
    const marker = { dir: "▸", link: "↪", file: "·" }[kind];
    row.append(
      el("span", "marker", marker),
      el("span", "name", entry.name),
      el("span", "size", entry.is_dir ? "" : humanSize(entry.size)),
      el("span", "mtime", formatMtime(entry.mtime)),
    );
    return row;
  }

  rows() {
    return this.list.querySelectorAll(".row:not(.head)");
  }

  paintSelection() {
    this.rows().forEach((row, index) => {
      row.classList.toggle("selected", this.selected.has(index));
      row.classList.toggle("cursor", index === this.cursor);
    });
  }

  rowIndex(event) {
    const row = event.target.closest(".row:not(.head)");
    return row ? Number(row.dataset.index) : null;
  }

  // ---- selection and keyboard ----------------------------------------------

  onClick(event) {
    const index = this.rowIndex(event);
    if (index === null) return;
    this.list.focus();
    if (event.shiftKey && this.anchor !== null) this.selectRange(this.anchor, index);
    else if (event.ctrlKey || event.metaKey) this.toggle(index);
    else this.selectOnly(index);
    this.cursor = index;
    this.paintSelection();
  }

  selectOnly(index) {
    this.selected = new Set([index]);
    this.anchor = index;
  }

  toggle(index) {
    if (this.selected.has(index)) this.selected.delete(index);
    else this.selected.add(index);
    this.anchor = index;
  }

  selectRange(from, to) {
    const [low, high] = from < to ? [from, to] : [to, from];
    this.selected = new Set();
    for (let i = low; i <= high; i += 1) this.selected.add(i);
  }

  selectAll() {
    this.selected = new Set(this.entries.keys());
    this.paintSelection();
  }

  onDoubleClick(event) {
    const index = this.rowIndex(event);
    if (index !== null) this.open(index);
  }

  open(index) {
    const entry = this.entries[index];
    if (entry && entry.is_dir) this.load(this.host, posixJoin(this.path, entry.name));
  }

  onKeyDown(event) {
    const handlers = {
      ArrowDown: () => this.moveCursor(1, event.shiftKey),
      ArrowUp: () => this.moveCursor(-1, event.shiftKey),
      Enter: () => this.cursor !== null && this.open(this.cursor),
      Backspace: () => this.up(),
    };
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "a") {
      event.preventDefault();
      this.selectAll();
    } else if (handlers[event.key]) {
      event.preventDefault();
      handlers[event.key]();
    }
  }

  moveCursor(step, extend) {
    if (this.entries.length === 0) return;
    const start = this.cursor === null ? (step > 0 ? -1 : this.entries.length) : this.cursor;
    const next = Math.min(this.entries.length - 1, Math.max(0, start + step));
    if (extend && this.anchor !== null) this.selectRange(this.anchor, next);
    else this.selectOnly(next);
    this.cursor = next;
    this.paintSelection();
    this.rows()[next].scrollIntoView({ block: "nearest" });
  }

  // Full remote paths of the selected rows.
  selectedPaths() {
    const indexes = [...this.selected].sort((a, b) => a - b);
    return indexes.map((i) => posixJoin(this.path, this.entries[i].name));
  }

  // ---- drag and drop -------------------------------------------------------

  onDragStart(event) {
    const index = this.rowIndex(event);
    if (index === null) return;
    if (!this.selected.has(index)) {
      this.selectOnly(index); // dragging an unselected row drags just that row
      this.cursor = index;
      this.paintSelection();
    }
    const payload = { host: this.host, paths: this.selectedPaths() };
    event.dataTransfer.setData(DRAG_TYPE, JSON.stringify(payload));
    event.dataTransfer.effectAllowed = "copy";
    dragSource = this;
  }

  onDragEnd() {
    dragSource = null;
    document.querySelectorAll(".drop-target, .drop-row").forEach((node) => {
      node.classList.remove("drop-target", "drop-row");
    });
  }

  // True when a hopsync drag from the OTHER pane is over us.
  acceptsDrag(event) {
    return dragSource !== null && dragSource !== this && event.dataTransfer.types.includes(DRAG_TYPE);
  }

  dropRow(event) {
    const row = event.target.closest(".row.dir");
    return row ? row : null;
  }

  onDragOver(event) {
    if (!this.acceptsDrag(event)) return;
    event.preventDefault(); // this is what allows a drop here
    event.dataTransfer.dropEffect = "copy";
    this.clearHighlight();
    const row = this.dropRow(event);
    if (row) row.classList.add("drop-row");
    else this.root.classList.add("drop-target");
  }

  onDragLeave(event) {
    if (!this.root.contains(event.relatedTarget)) this.clearHighlight();
  }

  clearHighlight() {
    this.root.classList.remove("drop-target");
    this.root.querySelectorAll(".drop-row").forEach((row) => row.classList.remove("drop-row"));
  }

  onDrop(event) {
    if (!this.acceptsDrag(event)) return;
    event.preventDefault();
    const row = this.dropRow(event);
    const target = row ? posixJoin(this.path, this.entries[Number(row.dataset.index)].name) : this.path;
    this.clearHighlight();
    const payload = JSON.parse(event.dataTransfer.getData(DRAG_TYPE));
    this.callbacks.onDrop(payload, this, target);
  }
}
