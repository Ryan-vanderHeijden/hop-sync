// Run with: node --test tests/web/format.test.mjs
import assert from "node:assert/strict";
import test from "node:test";

import {
  etaSeconds,
  formatEta,
  formatMtime,
  formatSpeed,
  humanSize,
  posixBasename,
  posixDirname,
  posixJoin,
  skipPhrase,
  SpeedTracker,
} from "../../hopsync/web/format.js";

test("humanSize", () => {
  assert.equal(humanSize(0), "0 B");
  assert.equal(humanSize(1023), "1023 B");
  assert.equal(humanSize(1536), "1.5 KB");
  assert.equal(humanSize(5 * 1024 * 1024), "5.0 MB");
  assert.equal(humanSize(3 * 1024 ** 3), "3.0 GB");
  assert.equal(humanSize(undefined), "0 B");
});

test("formatSpeed", () => {
  assert.equal(formatSpeed(2 * 1024 * 1024), "2.0 MB/s");
});

test("formatEta", () => {
  assert.equal(formatEta(0), "0:00");
  assert.equal(formatEta(65), "1:05");
  assert.equal(formatEta(3725), "1:02:05");
  assert.equal(formatEta(Infinity), "");
  assert.equal(formatEta(-1), "");
});

test("formatMtime uses local time, zero padded", () => {
  const epoch = new Date(2024, 4, 6, 7, 8).getTime() / 1000;
  assert.equal(formatMtime(epoch), "2024-05-06 07:08");
});

test("posix path helpers", () => {
  assert.equal(posixJoin("/home/me", "a.txt"), "/home/me/a.txt");
  assert.equal(posixJoin("/", "a"), "/a");
  assert.equal(posixJoin("/home/me/", "a"), "/home/me/a");
  assert.equal(posixJoin("", "a"), "a");
  assert.equal(posixDirname("/home/me/a.txt"), "/home/me");
  assert.equal(posixDirname("/home"), "/");
  assert.equal(posixDirname("/"), "/");
  assert.equal(posixDirname("a/b"), "a");
  assert.equal(posixDirname("a"), ".");
  assert.equal(posixDirname("/home/me/"), "/home");
  assert.equal(posixBasename("/home/me/a.txt"), "a.txt");
  assert.equal(posixBasename("/home/me/"), "me");
});

test("SpeedTracker measures over a sliding window", () => {
  const tracker = new SpeedTracker(5);
  assert.equal(tracker.rate(0), 0);
  tracker.add(0, 0);
  tracker.add(1000, 1);
  tracker.add(3000, 2);
  assert.equal(tracker.rate(2), 1500);
  // old samples fall out of the 5 second window
  tracker.add(13000, 10);
  tracker.add(14000, 11);
  assert.equal(tracker.rate(11), 1000);
});

test("SpeedTracker decays to zero when updates stop", () => {
  const tracker = new SpeedTracker(5);
  tracker.add(0, 0);
  tracker.add(5000, 1);
  assert.ok(tracker.rate(3) < tracker.rate(1));
  assert.equal(tracker.rate(100), 0);
});

test("etaSeconds only when the walk is complete and speed is known", () => {
  const job = { walk_complete: true, bytes_total: 1000, bytes_done: 400 };
  assert.equal(etaSeconds(job, 100), 6);
  assert.equal(etaSeconds(job, 0), null);
  assert.equal(etaSeconds({ ...job, walk_complete: false }, 100), null);
  assert.equal(etaSeconds({ ...job, bytes_done: 2000 }, 100), 0);
});

test("skipPhrase", () => {
  assert.equal(skipPhrase("already copied", 3), "3 already copied");
  assert.equal(skipPhrase("exists, skipped", 2), "2 skipped (exist)");
  assert.equal(skipPhrase("symlink", 1), "1 symlink skipped");
  assert.equal(skipPhrase("symlink", 3), "3 symlinks skipped");
  assert.equal(skipPhrase("something new", 2), "2 skipped (something new)");
});
