// The progress panel: one row per job, driven by WebSocket messages.

import { clear, el } from "./dom.js";
import { etaSeconds, formatEta, formatSpeed, humanSize, skipPhrase, SpeedTracker } from "./format.js";

const FINAL_STATES = ["done", "failed", "cancelled"];
// A rerun of a huge tree can skip 100k files; keep only this many paths per job.
export const MAX_SKIPPED_LIST = 500;

export class JobsPanel {
  // callbacks: { onCancel(id), onRetry(id), onJobFinished(job), onShowConflict(id), onUpdate(jobs) }
  constructor(root, callbacks) {
    this.root = root;
    this.callbacks = { onShowConflict() {}, onUpdate() {}, ...callbacks };
    this.jobs = new Map(); // id -> latest job
    this.entries = new Map(); // id -> { skipCounts: Map(reason -> n), skipped: [{path, reason}] (capped), skippedTotal, failed: [{path, reason}] }
    this.trackers = new Map(); // id -> SpeedTracker
    this.hidden = new Set(); // finished jobs the user cleared (client-side only)
    this.expanded = new Set(); // jobs whose failed list is open
    this.expandedSkips = new Set(); // jobs whose skipped list is open
    this.renderQueued = false;
    this.buildDom();
    // Speed and ETA change with time even when no message arrives.
    setInterval(() => this.hasRunning() && this.scheduleRender(), 1000);
  }

  buildDom() {
    const header = el("div", "jobs-header");
    this.title = el("strong", "", "Transfers");
    this.connection = el("span", "connection", "");
    this.clearButton = el("button", "", "Clear finished");
    this.toggleButton = el("button", "", "Collapse");
    header.append(this.title, this.connection, this.clearButton, this.toggleButton);
    this.body = el("div", "jobs-body");
    this.root.append(header, this.body);

    this.clearButton.addEventListener("click", () => this.clearFinished());
    this.toggleButton.addEventListener("click", () => {
      const collapsed = this.root.classList.toggle("collapsed");
      this.toggleButton.textContent = collapsed ? "Expand" : "Collapse";
    });
  }

  setConnectionStatus(status) {
    this.connection.textContent = status === "connected" ? "" : "reconnecting…";
  }

  // ---- incoming messages ---------------------------------------------------

  handleMessage(message) {
    if (message.type === "snapshot") this.applySnapshot(message.jobs);
    else if (message.type === "job") this.applyJob(message.job);
    else if (message.type === "entry") this.applyEntry(message);
    this.scheduleRender();
    this.callbacks.onUpdate(this.jobs);
  }

  // A snapshot replaces the job list. Entry details cannot be re-sent by
  // the server, so we keep them for jobs that still exist.
  applySnapshot(jobs) {
    const previous = this.jobs;
    this.jobs = new Map();
    for (const job of jobs) {
      this.jobs.set(job.id, job);
      this.track(job);
    }
    for (const id of [...this.entries.keys()]) if (!this.jobs.has(id)) this.entries.delete(id);
    for (const id of [...this.trackers.keys()]) if (!this.jobs.has(id)) this.trackers.delete(id);
    for (const job of jobs) this.noticeFinish(previous.get(job.id), job);
  }

  applyJob(job) {
    const before = this.jobs.get(job.id);
    this.jobs.set(job.id, job);
    if (before && before.attempt !== job.attempt) this.entries.delete(job.id); // a retry starts clean
    this.track(job);
    this.noticeFinish(before, job);
  }

  track(job) {
    if (!this.trackers.has(job.id)) this.trackers.set(job.id, new SpeedTracker());
    this.trackers.get(job.id).add(job.bytes_transferred, performance.now() / 1000);
  }

  // Tell the app when a job just reached done or failed (it may refresh panes).
  noticeFinish(before, job) {
    if (!before) return; // first time we hear of it: nothing "just" happened
    const wasFinal = ["done", "failed"].includes(before.state);
    if (["done", "failed"].includes(job.state) && !wasFinal) this.callbacks.onJobFinished(job);
  }

  applyEntry(message) {
    if (!this.entries.has(message.job_id)) {
      this.entries.set(message.job_id, { skipCounts: new Map(), skipped: [], skippedTotal: 0, failed: [] });
    }
    const record = this.entries.get(message.job_id);
    if (message.kind === "skipped") this.addSkipped(record, message);
    else record.failed.push({ path: message.path, reason: message.reason });
  }

  // Counts stay exact; only the list of paths is capped.
  addSkipped(record, message) {
    record.skippedTotal += 1;
    record.skipCounts.set(message.reason, (record.skipCounts.get(message.reason) || 0) + 1);
    if (record.skipped.length < MAX_SKIPPED_LIST) record.skipped.push({ path: message.path, reason: message.reason });
  }

  clearFinished() {
    for (const job of this.jobs.values()) if (job.state === "done") this.hidden.add(job.id);
    this.scheduleRender();
  }

  hasRunning() {
    return [...this.jobs.values()].some((job) => job.state === "running");
  }

  // ---- drawing -------------------------------------------------------------

  scheduleRender() {
    if (this.renderQueued) return;
    this.renderQueued = true;
    requestAnimationFrame(() => {
      this.renderQueued = false;
      this.render();
    });
  }

  render() {
    clear(this.body);
    const visible = [...this.jobs.values()].filter((job) => !this.hidden.has(job.id)).reverse();
    this.title.textContent = `Transfers (${visible.length})`;
    if (visible.length === 0) this.body.append(el("div", "empty", "No transfers yet. Drag files between the panes."));
    for (const job of visible) this.body.append(this.makeJobRow(job));
  }

  makeJobRow(job) {
    const row = el("div", `job ${job.state}`);
    const top = el("div", "job-top");
    top.append(
      el("code", "job-id", job.id),
      el("span", "job-route", `${job.src.host}:${job.src.path} -> ${job.dst.host}:${job.dst.path}`),
      el("span", `badge ${job.state}`, job.state),
      this.makeButtons(job),
    );
    row.append(top, this.makeProgress(job), this.makeDetails(job));
    for (const list of [this.makeSkippedList(job), this.makeFailedList(job)]) if (list) row.append(list);
    return row;
  }

  makeButtons(job) {
    const box = el("span", "job-buttons");
    if (job.state === "queued" || job.state === "running") {
      const cancel = el("button", "", "Cancel");
      cancel.addEventListener("click", () => this.callbacks.onCancel(job.id));
      box.append(cancel);
    }
    if (job.state === "failed" || job.state === "cancelled") {
      const retry = el("button", "", "Retry");
      retry.addEventListener("click", () => this.callbacks.onRetry(job.id));
      box.append(retry);
    }
    return box;
  }

  makeProgress(job) {
    const wrap = el("div", "progress");
    const bar = el("div", "bar");
    const fill = el("div", "fill");
    // Setting style from JavaScript is allowed by our content security policy.
    fill.style.width = `${this.percent(job)}%`;
    bar.append(fill);
    const plus = job.walk_complete ? "" : "+"; // "+": the total may still grow
    const text = `${humanSize(job.bytes_done)} / ${humanSize(job.bytes_total)}${plus}`;
    wrap.append(bar, el("span", "progress-text", text));
    return wrap;
  }

  percent(job) {
    if (job.state === "done") return 100;
    if (!job.bytes_total) return 0;
    return Math.min(100, (100 * job.bytes_done) / job.bytes_total);
  }

  makeDetails(job) {
    const details = el("div", "job-details");
    const now = performance.now() / 1000;
    if (job.state === "running") {
      const speed = this.trackers.get(job.id).rate(now);
      details.append(el("span", "", formatSpeed(speed)));
      const eta = etaSeconds(job, speed);
      if (eta !== null) details.append(el("span", "", `ETA ${formatEta(eta)}`));
      if (job.current_file) details.append(el("span", "current-file", job.current_file));
    }
    if (job.pending_conflict) {
      const waiting = el("button", "link-button waiting", "1 decision waiting");
      waiting.addEventListener("click", () => this.callbacks.onShowConflict(job.id));
      details.append(waiting);
    }
    if (job.message) details.append(el("span", `message ${job.state}`, job.message));
    const record = this.entries.get(job.id);
    const identical = this.identicalMessage(job, record);
    if (identical) details.append(el("span", "message", identical));
    if (record && record.skippedTotal) details.append(this.makeSkippedToggle(job, record));
    if (record && record.failed.length) details.append(this.makeFailedToggle(job, record));
    return details;
  }

  // A single-file job (it never walks a tree) whose one entry was skipped.
  identicalMessage(job, record) {
    if (job.state !== "done" || job.walk_complete || !record || record.skippedTotal !== 1) return "";
    if (record.skipCounts.has("already copied")) return "already on destination (same size and time)";
    return record.skipCounts.has("exists, skipped") ? "skipped: a different file exists on the destination" : "";
  }

  makeSkippedToggle(job, record) {
    const text = [...record.skipCounts].map(([reason, count]) => skipPhrase(reason, count)).join(", ");
    const button = el("button", "link-button skipped-toggle", text);
    button.addEventListener("click", () => {
      if (this.expandedSkips.has(job.id)) this.expandedSkips.delete(job.id);
      else this.expandedSkips.add(job.id);
      this.scheduleRender();
    });
    return button;
  }

  makeSkippedList(job) {
    const record = this.entries.get(job.id);
    if (!record || !this.expandedSkips.has(job.id)) return null;
    const list = el("ul", "skipped-list");
    for (const item of record.skipped) list.append(el("li", "", `${item.path}: ${item.reason}`));
    const hidden = record.skippedTotal - record.skipped.length;
    if (hidden > 0) list.append(el("li", "more", `and ${hidden} more`));
    return list;
  }

  makeFailedToggle(job, record) {
    const button = el("button", "link-button", `${record.failed.length} failed`);
    button.addEventListener("click", () => {
      if (this.expanded.has(job.id)) this.expanded.delete(job.id);
      else this.expanded.add(job.id);
      this.scheduleRender();
    });
    return button;
  }

  makeFailedList(job) {
    const record = this.entries.get(job.id);
    if (!record || !this.expanded.has(job.id)) return null;
    const list = el("ul", "failed-list");
    for (const item of record.failed) list.append(el("li", "", `${item.path}: ${item.reason}`));
    return list;
  }
}
