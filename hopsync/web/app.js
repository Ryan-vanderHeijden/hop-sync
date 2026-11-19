// Wires the panes, the progress panel, drag-and-drop and copy/paste together.

import { cancelJob, connectEvents, createJob, getHosts, onSessionEnded, retryJob } from "./api.js";
import { el } from "./dom.js";
import { posixDirname } from "./format.js";
import { JobsPanel } from "./jobs.js";
import { Pane } from "./pane.js";

const $ = (id) => document.getElementById(id);

let clipboard = null; // { host, paths }: our own "copy" buffer, not the system clipboard
let panes = [];
let jobsPanel = null;

// ---- small helpers ---------------------------------------------------------

function toast(text) {
  const box = el("div", "toast", text);
  $("toasts").append(box);
  setTimeout(() => box.remove(), 4500);
}

// The three checkboxes. Overwrite is deliberately never remembered.
function currentOptions() {
  return {
    verify: $("opt-verify").checked,
    links: $("opt-links").checked,
    overwrite: $("opt-overwrite").checked,
  };
}

function resetOptions() {
  for (const id of ["opt-verify", "opt-links", "opt-overwrite"]) $(id).checked = false;
}

function focusedPane() {
  return panes.find((pane) => pane.root.contains(document.activeElement)) || null;
}

// ---- starting jobs ---------------------------------------------------------

// One job per item; the server puts each item inside the target directory.
async function enqueue(payload, targetPane, targetDirectory) {
  if (payload.host === targetPane.host) {
    toast("Both panes are on the same host. Pick a different host in one of them.");
    return;
  }
  const options = currentOptions();
  const destination = { host: targetPane.host, path: targetDirectory };
  let queued = 0;
  let lastError = "";
  for (const path of payload.paths) {
    try {
      await createJob({ host: payload.host, path }, destination, options);
      queued += 1;
    } catch (error) {
      lastError = error.message;
    }
  }
  if (lastError) toast(queued ? `${queued} queued, some failed: ${lastError}` : lastError);
  else toast(`${queued} job${queued === 1 ? "" : "s"} queued`);
}

function otherThan(pane) {
  return panes.find((candidate) => candidate !== pane);
}

// ---- keyboard: copy and paste between panes ---------------------------------

function isTyping(target) {
  return ["INPUT", "SELECT", "TEXTAREA"].includes(target.tagName);
}

function onKeyDown(event) {
  if (isTyping(event.target) || !(event.ctrlKey || event.metaKey)) return;
  const pane = focusedPane();
  if (!pane) return;
  const key = event.key.toLowerCase();
  if (key === "c" && pane.selected.size > 0) {
    event.preventDefault();
    clipboard = { host: pane.host, paths: pane.selectedPaths() };
    toast(`${clipboard.paths.length} item${clipboard.paths.length === 1 ? "" : "s"} copied — paste in the other pane`);
  } else if (key === "v" && clipboard) {
    event.preventDefault();
    enqueue(clipboard, pane, pane.path);
  }
}

// ---- reacting to finished jobs ----------------------------------------------

// Refresh any pane that shows the job's destination directory or its parent.
function refreshPanesFor(job) {
  const target = job.dst.path;
  for (const pane of panes) {
    const showsTarget = pane.path === target || pane.path === posixDirname(target);
    if (pane.host === job.dst.host && showsTarget) pane.refresh();
  }
}

function reportError(error) {
  toast(error.message);
}

// ---- start up ---------------------------------------------------------------

async function updateHostStatus() {
  try {
    const hosts = await getHosts();
    panes.forEach((pane) => pane.setHosts(hosts));
  } catch {
    // the dots just stay as they were
  }
}

function startingPlace(side, aliases, fallbackIndex) {
  const saved = Pane.recall(side);
  if (aliases.includes(saved.host)) return saved;
  return { host: aliases[Math.min(fallbackIndex, aliases.length - 1)], path: "" };
}

function createPanes() {
  const callbacks = {
    onLoaded: () => updateHostStatus(),
    onDrop: (payload, pane, directory) => enqueue(payload, pane, directory),
  };
  return [new Pane($("pane-left"), "left", callbacks), new Pane($("pane-right"), "right", callbacks)];
}

async function start() {
  resetOptions();
  window.addEventListener("pageshow", resetOptions); // browsers may restore form state
  onSessionEnded(() => ($("banner").hidden = false));
  panes = createPanes();
  jobsPanel = new JobsPanel($("jobs"), {
    onCancel: (id) => cancelJob(id).catch(reportError),
    onRetry: (id) => retryJob(id).catch(reportError),
    onJobFinished: refreshPanesFor,
  });
  document.addEventListener("keydown", onKeyDown);

  let hosts;
  try {
    hosts = await getHosts();
  } catch (error) {
    toast(error.message);
    return;
  }
  const aliases = hosts.map((h) => h.alias);
  if (aliases.length === 0) {
    toast("No hosts found in your ssh config.");
    return;
  }
  panes.forEach((pane) => pane.setHosts(hosts));
  [startingPlace("left", aliases, 0), startingPlace("right", aliases, 1)].forEach((place, i) => {
    panes[i].load(place.host, place.path || "");
  });
  connectEvents({
    onMessage: (message) => jobsPanel.handleMessage(message),
    onStatus: (status) => jobsPanel.setConnectionStatus(status),
  });
}

start();
