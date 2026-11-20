// Smoke tests for pane.js and jobs.js using a tiny fake DOM (see fakedom.mjs).
// Run with: node --test tests/web/ui.test.mjs
import assert from "node:assert/strict";
import test from "node:test";

import { FakeElement, installFakeDom } from "./fakedom.mjs";

installFakeDom();

const listing = {
  host: "hpc",
  path: ".",
  abs_path: "/home/me",
  entries: [
    { name: "adir", size: 0, mtime: 1700000000, is_dir: true, is_link: false },
    { name: "<b>evil</b>.txt", size: 2048, mtime: 1700000000, is_dir: false, is_link: false },
    { name: "other.bin", size: 5, mtime: 1700000000, is_dir: false, is_link: false },
  ],
};

let requests = [];
let lastInit = null;
globalThis.fetch = async (url, init) => {
  requests.push(String(url));
  lastInit = init;
  return { ok: true, status: 200, json: async () => listing };
};

const { Pane, DRAG_TYPE } = await import("../../hopsync/web/pane.js");
const { JobsPanel, MAX_SKIPPED_LIST } = await import("../../hopsync/web/jobs.js");
const { ConflictDialog } = await import("../../hopsync/web/conflict.js");
const { resolveConflict } = await import("../../hopsync/web/api.js");

function makePane() {
  const calls = { dropped: [], loaded: 0 };
  const pane = new Pane(new FakeElement("section"), "left", {
    onLoaded: () => { calls.loaded += 1; },
    onDrop: (payload, target, directory) => calls.dropped.push({ payload, directory }),
  });
  pane.setHosts([{ alias: "hpc", connected: true, error_kind: null }, { alias: "server", connected: false, error_kind: null }]);
  return { pane, calls };
}

const rowsOf = (pane) => pane.rows();

test("pane lists a directory and shows names as plain text", async () => {
  const { pane, calls } = makePane();
  await pane.load("hpc", "");
  assert.match(requests.at(-1), /^\/api\/list\?host=hpc&path=$/);
  assert.equal(rowsOf(pane).length, 3);
  assert.equal(pane.pathInput.value, "/home/me");
  assert.ok(pane.list.textContent.includes("<b>evil</b>.txt")); // kept as text, never HTML
  assert.equal(calls.loaded, 1);
  assert.equal(pane.dot.className, "dot connected");
});

test("selection: click, ctrl+click, shift+click, ctrl+a, keys", async () => {
  const { pane } = makePane();
  await pane.load("hpc", "");
  const click = (i, mods = {}) => rowsOf(pane)[i].dispatch("click", { ...mods });
  click(1);
  assert.deepEqual([...pane.selected], [1]);
  click(2, { ctrlKey: true });
  assert.deepEqual([...pane.selected].sort(), [1, 2]);
  click(0, { shiftKey: true }); // the range starts at the last ctrl+click (row 2)
  assert.deepEqual([...pane.selected].sort(), [0, 1, 2]);
  pane.list.dispatch("keydown", { key: "a", ctrlKey: true });
  assert.equal(pane.selected.size, 3);
  pane.list.dispatch("keydown", { key: "ArrowDown" });
  assert.equal(pane.selected.size, 1);
  assert.deepEqual(pane.selectedPaths(), ["/home/me/<b>evil</b>.txt"]); // the cursor was on row 0
});

test("double click opens a directory; Backspace goes up", async () => {
  const { pane } = makePane();
  await pane.load("hpc", "");
  rowsOf(pane)[0].dispatch("dblclick");
  await new Promise((r) => setImmediate(r));
  assert.match(requests.at(-1), /path=%2Fhome%2Fme%2Fadir/);
  pane.list.dispatch("keydown", { key: "Backspace" });
  // (the fake server always answers /home/me, so "up" is /home)
  assert.match(requests.at(-1), /path=%2Fhome$/);
});

test("a newer listing makes an older one irrelevant", async () => {
  const { pane } = makePane();
  const slow = pane.load("hpc", "first");
  await pane.load("hpc", "second");
  await slow;
  assert.equal(pane.requestId, 2);
});

test("dragging starts with the selection and drops on the other pane", async () => {
  const left = makePane();
  const right = makePane();
  await left.pane.load("hpc", "");
  await right.pane.load("server", "");
  const stored = {};
  const dataTransfer = { types: [DRAG_TYPE], setData: (t, v) => { stored[t] = v; }, getData: (t) => stored[t] };
  // an unselected row drags just itself
  rowsOf(left.pane)[2].dispatch("dragstart", { dataTransfer });
  assert.deepEqual(JSON.parse(stored[DRAG_TYPE]), { host: "hpc", paths: ["/home/me/other.bin"] });
  // dropping on the source pane does nothing
  left.pane.root.dispatch("drop", { dataTransfer });
  assert.equal(left.calls.dropped.length, 0);
  // dropping on a directory row of the other pane targets that directory
  rowsOf(right.pane)[0].dispatch("dragover", { dataTransfer });
  assert.ok(rowsOf(right.pane)[0].classList.contains("drop-row"));
  rowsOf(right.pane)[0].dispatch("drop", { dataTransfer });
  assert.equal(right.calls.dropped[0].directory, "/home/me/adir");
  // dropping on empty space targets the current directory
  right.pane.root.dispatch("drop", { dataTransfer, target: right.pane.list });
  assert.equal(right.calls.dropped[1].directory, "/home/me");
});

function job(overrides = {}) {
  return {
    id: "abc12345", src: { host: "hpc", path: "/data/x" }, dst: { host: "server", path: "/out" },
    options: {}, state: "running", bytes_done: 500, bytes_total: 1000, bytes_transferred: 500,
    files_done: 0, files_found: 1, walk_complete: false, current_file: "/data/x/file<1>",
    message: "", error_kind: null, error_alias: null, attempt: 1, ...overrides,
  };
}

test("jobs panel shows progress, a growing total, entries and buttons", () => {
  const finished = [];
  const actions = [];
  const panel = new JobsPanel(new FakeElement("section"), {
    onCancel: (id) => actions.push(["cancel", id]),
    onRetry: (id) => actions.push(["retry", id]),
    onJobFinished: (j) => finished.push(j.state),
  });
  panel.handleMessage({ type: "snapshot", jobs: [job()] });
  let text = panel.body.textContent;
  assert.ok(text.includes("hpc:/data/x -> server:/out"));
  assert.ok(text.includes("500 B / 1000 B+")); // "+" while the walk is not complete
  assert.ok(text.includes("/data/x/file<1>"));

  panel.handleMessage({ type: "entry", job_id: "abc12345", kind: "skipped", path: "a", reason: "already copied" });
  panel.handleMessage({ type: "entry", job_id: "abc12345", kind: "failed", path: "b.txt", reason: "denied" });
  text = panel.body.textContent;
  assert.ok(text.includes("1 already copied") && text.includes("1 failed"));

  const buttons = () => panel.body.all().filter((n) => n.tagName === "BUTTON");
  buttons().find((b) => b.textContent === "Cancel").dispatch("click");
  buttons().find((b) => b.textContent === "1 failed").dispatch("click");
  assert.ok(panel.body.textContent.includes("b.txt: denied"));

  panel.handleMessage({ type: "job", job: job({ state: "failed", message: "1 entries failed", walk_complete: true }) });
  assert.deepEqual(finished, ["failed"]);
  buttons().find((b) => b.textContent === "Retry").dispatch("click");
  assert.deepEqual(actions, [["cancel", "abc12345"], ["retry", "abc12345"]]);

  panel.handleMessage({ type: "job", job: job({ id: "done0001", state: "done", walk_complete: true }) });
  panel.clearButton.dispatch("click");
  assert.ok(!panel.body.textContent.includes("done0001"));
  assert.ok(panel.body.textContent.includes("abc12345"));
});

test("jobs panel: ETA only for a running job with a complete walk and known speed", () => {
  const panel = new JobsPanel(new FakeElement("section"), { onCancel() {}, onRetry() {}, onJobFinished() {} });
  panel.handleMessage({ type: "snapshot", jobs: [job({ walk_complete: true, bytes_transferred: 0 })] });
  assert.ok(!panel.body.textContent.includes("ETA"));
});

function jobWithConflict(id = "abc12345", conflictId = "c1") {
  return job({
    id,
    pending_conflict: {
      conflict_id: conflictId, job_id: id, path: "/out/dir/report <1>.txt",
      src_size: 2048, src_mtime: 1700000000, dst_size: 10, dst_mtime: 1600000000,
    },
  });
}

const buttonsIn = (dialog) => dialog.all().filter((n) => n.tagName === "BUTTON");

test("conflict dialog shows both files, focuses Skip and posts the decision", async () => {
  const decisions = [];
  const dialog = new FakeElement("dialog");
  const view = new ConflictDialog(dialog, {
    onDecide: (...args) => { decisions.push(args); return resolveConflict(...args); },
  });
  view.sync(new Map([["abc12345", jobWithConflict()]]));
  assert.equal(dialog.open, true);
  const text = dialog.textContent;
  assert.ok(text.includes("report <1>.txt") && text.includes("2.0 KB") && text.includes("10 B"));
  assert.equal(globalThis.document.activeElement.textContent, "Skip"); // never Overwrite
  const names = buttonsIn(dialog).map((b) => b.textContent);
  assert.deepEqual(names, ["Overwrite", "Skip", "Keep both"]);

  const checkbox = dialog.all().find((n) => n.tagName === "INPUT");
  checkbox.checked = true;
  buttonsIn(dialog).find((b) => b.textContent === "Skip").dispatch("click");
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(decisions, [["abc12345", "c1", "skip", true]]);
  assert.equal(requests.at(-1), "/api/jobs/abc12345/conflict");
  assert.equal(lastInit.method, "POST");
  assert.deepEqual(JSON.parse(lastInit.body), { conflict_id: "c1", decision: "skip", apply_to_all: true });
  assert.equal(dialog.open, false);

  // the server still lists it for a moment: the dialog must not pop up again
  view.sync(new Map([["abc12345", jobWithConflict()]]));
  assert.equal(dialog.open, false);
});

test("conflict dialog: Escape keeps the job waiting; the badge reopens; jobs go one at a time", () => {
  const dialog = new FakeElement("dialog");
  const view = new ConflictDialog(dialog, { onDecide() {} });
  const jobs = new Map([
    ["first000", jobWithConflict("first000", "c1")],
    ["second00", jobWithConflict("second00", "c2")],
  ]);
  view.sync(jobs);
  assert.equal(view.current.conflict_id, "c1"); // queue order
  dialog.close(); // what Escape does: closes without a decision
  assert.equal(view.current.conflict_id, "c2"); // the next waiting job
  dialog.close();
  assert.equal(view.current, null);
  view.sync(jobs);
  assert.equal(dialog.open, false); // dismissed ones stay closed
  view.reopen("first000");
  assert.equal(dialog.open, true);
  assert.equal(view.current.conflict_id, "c1");
  // the job is cancelled meanwhile: the dialog goes away by itself
  view.sync(new Map([["first000", job({ id: "first000", state: "cancelled" })]]));
  assert.equal(dialog.open, false);
});

test("jobs panel shows a 'decision waiting' badge that reopens the dialog", () => {
  const shown = [];
  const panel = new JobsPanel(new FakeElement("section"), {
    onCancel() {}, onRetry() {}, onJobFinished() {}, onShowConflict: (id) => shown.push(id),
  });
  panel.handleMessage({ type: "snapshot", jobs: [jobWithConflict()] });
  const badge = panel.body.all().find((n) => n.tagName === "BUTTON" && n.textContent === "1 decision waiting");
  badge.dispatch("click");
  assert.deepEqual(shown, ["abc12345"]);
});

function panelWith(jobOverrides = {}) {
  const panel = new JobsPanel(new FakeElement("section"), { onCancel() {}, onRetry() {}, onJobFinished() {} });
  panel.handleMessage({ type: "snapshot", jobs: [job(jobOverrides)] });
  return panel;
}

const skip = (panel, path, reason, id = "abc12345") =>
  panel.handleMessage({ type: "entry", job_id: id, kind: "skipped", path, reason });
const buttonNamed = (panel, text) => panel.body.all().find((n) => n.tagName === "BUTTON" && n.textContent === text);

test("skipped entries are summarised per reason", () => {
  const panel = panelWith({ state: "done", walk_complete: true });
  skip(panel, "a", "already copied");
  skip(panel, "b", "already copied");
  skip(panel, "c", "exists, skipped");
  skip(panel, "d", "symlink");
  skip(panel, "e", "special file");
  const summary = panel.body.all().find((n) => n.className.includes("skipped-toggle")).textContent;
  assert.equal(summary, "2 already copied, 1 skipped (exist), 1 symlink skipped, 1 special file skipped");
});

test("the skipped summary expands to every path with its reason", () => {
  const panel = panelWith({ state: "done", walk_complete: true });
  skip(panel, "/out/a.txt", "already copied");
  skip(panel, "/out/<b>.txt", "exists, skipped");
  assert.ok(!panel.body.textContent.includes("/out/a.txt"));
  buttonNamed(panel, "1 already copied, 1 skipped (exist)").dispatch("click");
  const text = panel.body.textContent;
  assert.ok(text.includes("/out/a.txt: already copied") && text.includes("/out/<b>.txt: exists, skipped"));
  buttonNamed(panel, "1 already copied, 1 skipped (exist)").dispatch("click"); // closes again
  assert.ok(!panel.body.textContent.includes("/out/a.txt: already copied"));
});

test("the stored list is capped but the counts stay exact", () => {
  const panel = panelWith({ state: "done", walk_complete: true });
  const total = MAX_SKIPPED_LIST + 37;
  for (let i = 0; i < total; i += 1) skip(panel, `/f/${i}`, "already copied");
  assert.equal(panel.entries.get("abc12345").skipped.length, MAX_SKIPPED_LIST);
  buttonNamed(panel, `${total} already copied`).dispatch("click");
  const items = panel.body.all().filter((n) => n.tagName === "LI");
  assert.equal(items.length, MAX_SKIPPED_LIST + 1);
  assert.equal(items.at(-1).textContent, "and 37 more");
});

test("a single file that was already on the destination says so", () => {
  const panel = panelWith({ state: "done", walk_complete: false, files_found: 0 });
  skip(panel, "/data/x", "already copied");
  assert.ok(panel.body.textContent.includes("already on destination (same size and time)"));
  // a folder copy with one identical file does not get that single-file wording
  const tree = panelWith({ state: "done", walk_complete: true });
  skip(tree, "/data/x/a", "already copied");
  assert.ok(!tree.body.textContent.includes("already on destination"));
});

test("a single file skipped because a different file exists is explained too", () => {
  const panel = panelWith({ state: "done", walk_complete: false, files_found: 0 });
  skip(panel, "/data/x", "exists, skipped");
  assert.ok(panel.body.textContent.includes("a different file exists on the destination"));
});
