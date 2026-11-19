// Pure helpers (no DOM), so they can be tested with `node --test`.

const UNITS = ["B", "KB", "MB", "GB", "TB"];

// 1536 -> "1.5 KB"
export function humanSize(bytes) {
  let value = Number(bytes) || 0;
  let unit = 0;
  while (value >= 1024 && unit < UNITS.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return unit === 0 ? `${Math.round(value)} B` : `${value.toFixed(1)} ${UNITS[unit]}`;
}

// bytes per second -> "12.3 MB/s"
export function formatSpeed(bytesPerSecond) {
  return `${humanSize(bytesPerSecond)}/s`;
}

// seconds -> "3:05" or "1:02:03"
export function formatEta(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "";
  const total = Math.round(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  const two = (n) => String(n).padStart(2, "0");
  return hours > 0 ? `${hours}:${two(minutes)}:${two(secs)}` : `${minutes}:${two(secs)}`;
}

// epoch seconds -> "2024-05-06 07:08" in the browser's local time
export function formatMtime(epochSeconds) {
  const date = new Date(epochSeconds * 1000);
  const two = (n) => String(n).padStart(2, "0");
  return (
    `${date.getFullYear()}-${two(date.getMonth() + 1)}-${two(date.getDate())} ` +
    `${two(date.getHours())}:${two(date.getMinutes())}`
  );
}

// Remote paths are always POSIX, whatever system this browser runs on.
export function posixJoin(directory, name) {
  if (directory === "" || directory === ".") return name;
  return directory.endsWith("/") ? directory + name : `${directory}/${name}`;
}

export function posixDirname(path) {
  const trimmed = path.length > 1 ? path.replace(/\/+$/, "") : path;
  const slash = trimmed.lastIndexOf("/");
  if (slash === -1) return ".";
  return slash === 0 ? "/" : trimmed.slice(0, slash);
}

export function posixBasename(path) {
  const trimmed = path.length > 1 ? path.replace(/\/+$/, "") : path;
  return trimmed.slice(trimmed.lastIndexOf("/") + 1);
}

// Speed over a sliding window (default 5 s), from a growing byte counter.
export class SpeedTracker {
  constructor(windowSeconds = 5) {
    this.windowSeconds = windowSeconds;
    this.samples = []; // {time, bytes}, oldest first
  }

  add(bytes, now) {
    this.samples.push({ time: now, bytes });
    this.prune(now);
  }

  prune(now) {
    while (this.samples.length > 0 && now - this.samples[0].time > this.windowSeconds) {
      this.samples.shift();
    }
  }

  // Using `now` as the end of the window makes the speed fall to 0 when
  // updates stop arriving.
  rate(now) {
    this.prune(now);
    if (this.samples.length < 2) return 0;
    const first = this.samples[0];
    const last = this.samples[this.samples.length - 1];
    const seconds = Math.max(now, last.time) - first.time;
    return seconds > 0 ? Math.max(0, last.bytes - first.bytes) / seconds : 0;
  }
}

// Seconds left, or null when it cannot be known yet.
export function etaSeconds(job, bytesPerSecond) {
  if (!job.walk_complete || bytesPerSecond <= 0) return null;
  return Math.max(0, job.bytes_total - job.bytes_done) / bytesPerSecond;
}
