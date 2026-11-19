// The "file exists" dialog. One conflict is shown at a time, in queue order.

import { clear, el } from "./dom.js";
import { formatMtime, humanSize, posixBasename } from "./format.js";

export class ConflictDialog {
  // callbacks: { onDecide(jobId, conflictId, decision, applyToAll) }
  constructor(dialog, callbacks) {
    this.dialog = dialog;
    this.callbacks = callbacks;
    this.jobs = new Map();
    this.current = null; // the conflict being shown
    this.dismissed = new Set(); // closed with Escape: the job still waits, the badge reopens it
    this.answered = new Set(); // sent to the server: ignore until the job reports it gone
    // "close" also fires when the user presses Escape (the browser closes the dialog).
    this.dialog.addEventListener("close", () => this.onClosed());
  }

  // Called whenever the job list changes.
  sync(jobs) {
    this.jobs = jobs;
    const waiting = this.waitingConflicts();
    if (this.current && !waiting.some((c) => c.conflict_id === this.current.conflict_id)) {
      this.finishCurrent(); // the job moved on (cancelled, answered elsewhere...)
    }
    if (!this.current) {
      const next = waiting.find((c) => !this.dismissed.has(c.conflict_id) && !this.answered.has(c.conflict_id));
      if (next) this.show(next);
    }
  }

  waitingConflicts() {
    return [...this.jobs.values()].filter((job) => job.pending_conflict).map((job) => job.pending_conflict);
  }

  // The "decision waiting" badge on a job row.
  reopen(jobId) {
    const job = this.jobs.get(jobId);
    if (!job || !job.pending_conflict) return;
    this.dismissed.delete(job.pending_conflict.conflict_id);
    if (!this.current) this.sync(this.jobs);
  }

  finishCurrent() {
    const wasOpen = this.dialog.open;
    this.current = null;
    if (wasOpen) this.dialog.close();
  }

  onClosed() {
    if (!this.current) return; // we closed it ourselves after a decision
    this.dismissed.add(this.current.conflict_id);
    this.current = null;
    this.sync(this.jobs); // maybe another job is waiting
  }

  show(conflict) {
    this.current = conflict;
    this.build(conflict);
    this.dialog.showModal();
    this.skipButton.focus(); // Skip is the safe default; Overwrite never gets the focus
  }

  build(conflict) {
    clear(this.dialog);
    const applyAll = el("input");
    applyAll.type = "checkbox";
    const label = el("label", "apply-all");
    label.append(applyAll, el("span", "", " Apply to all remaining conflicts in this job"));

    const buttons = el("div", "dialog-buttons");
    const overwrite = this.makeButton("Overwrite", "overwrite", conflict, applyAll, "danger");
    this.skipButton = this.makeButton("Skip", "skip", conflict, applyAll);
    const rename = this.makeButton("Keep both", "rename", conflict, applyAll);
    buttons.append(overwrite, this.skipButton, rename);

    this.dialog.append(
      el("h2", "", "File already exists"),
      el("p", "dialog-name", posixBasename(conflict.path)),
      el("p", "dialog-path", conflict.path),
      this.makeTable(conflict),
      label,
      buttons,
    );
  }

  makeTable(conflict) {
    const table = el("div", "compare");
    const cell = (text, className = "") => el("span", className, text);
    table.append(
      cell(""), cell("Source", "head"), cell("Destination (existing)", "head"),
      cell("Size", "head"), cell(humanSize(conflict.src_size)), cell(humanSize(conflict.dst_size)),
      cell("Modified", "head"), cell(formatMtime(conflict.src_mtime)), cell(formatMtime(conflict.dst_mtime)),
    );
    return table;
  }

  makeButton(text, decision, conflict, applyAll, extraClass = "") {
    const button = el("button", extraClass, text);
    button.addEventListener("click", () => {
      this.answered.add(conflict.conflict_id);
      this.callbacks.onDecide(conflict.job_id, conflict.conflict_id, decision, applyAll.checked);
      this.finishCurrent();
    });
    return button;
  }
}
