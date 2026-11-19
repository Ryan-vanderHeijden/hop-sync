// The progress panel: one row per job, driven by WebSocket messages.

import { clear, el } from "./dom.js";
import { etaSeconds, formatEta, formatSpeed, humanSize, SpeedTracker } from "./format.js";

const FINAL_STATES = ["done", "failed", "cancelled"];

export class JobsPanel {
  // callbacks: { onCancel(id), onRetry(id), onJobFinished(job) }
  constructor(root, callbacks) {
    this.root = root;
    this.callbacks = callbacks;
    this.jobs = new Map(); // id -> latest job
    this.entries = new Map(); // id -> { skipped: number, failed: [{path, reason}] }
    this.trackers = new Map(); // id -> SpeedTracker
    this.hidden = new Set(); // finished jobs the user cleared (client-side only)
    this.expanded = new Set(); // jobs whose failed list is open
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
    if (!this.entries.has(message.job_id)) this.entries.set(message.job_id, { skipped: 0, failed: [] });
    const record = this.entries.get(message.job_id);
    if (message.kind === "skipped") record.skipped += 1;
    else record.failed.push({ path: message.path, reason: message.reason });
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
    const failed = this.makeFailedList(job);
    if (failed) row.append(failed);
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
    if (job.message) details.append(el("span", `message ${job.state}`, job.message));
    const record = this.entries.get(job.id);
    if (record && record.skipped) details.append(el("span", "", `${record.skipped} skipped`));
    if (record && record.failed.length) details.append(this.makeFailedToggle(job, record));
    return details;
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
