// Talking to the server: fetch wrappers and the event WebSocket.

export class ApiError extends Error {
  constructor(message, status, kind) {
    super(message);
    this.status = status;
    this.kind = kind;
  }
}

let sessionEndedHandler = () => {};

// Called when the server says 401: it was restarted and our cookie is stale.
export function onSessionEnded(handler) {
  sessionEndedHandler = handler;
}

async function request(method, url, body, signal) {
  const options = { method, signal, credentials: "same-origin" };
  if (body !== undefined) {
    options.headers = { "Content-Type": "application/json" };
    options.body = JSON.stringify(body);
  }
  const response = await fetch(url, options);
  if (response.status === 401) sessionEndedHandler();
  if (!response.ok) throw await errorFrom(response);
  return response.json();
}

async function errorFrom(response) {
  try {
    const data = await response.json();
    return new ApiError(data.message || response.statusText, response.status, data.error);
  } catch {
    return new ApiError(response.statusText || "request failed", response.status, "http");
  }
}

export const getHosts = () => request("GET", "/api/hosts");

export function listDir(host, path, signal) {
  const query = new URLSearchParams({ host, path });
  return request("GET", `/api/list?${query}`, undefined, signal);
}

export const createJob = (src, dst, options) => request("POST", "/api/jobs", { src, dst, ...options });
export const cancelJob = (id) => request("DELETE", `/api/jobs/${encodeURIComponent(id)}`);
export const resolveConflict = (id, conflictId, decision, applyToAll) =>
  request("POST", `/api/jobs/${encodeURIComponent(id)}/conflict`, {
    conflict_id: conflictId,
    decision,
    apply_to_all: applyToAll,
  });
export const retryJob = (id) => request("POST", `/api/jobs/${encodeURIComponent(id)}/retry`);

// Opens /api/events and reconnects with a growing delay (1 s up to 10 s).
// The server sends a fresh snapshot after every connect.
export function connectEvents({ onMessage, onStatus }) {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  let delay = 1000;

  function open() {
    const socket = new WebSocket(`${scheme}://${location.host}/api/events`);
    socket.onopen = () => {
      delay = 1000;
      onStatus("connected");
    };
    socket.onmessage = (event) => onMessage(JSON.parse(event.data));
    socket.onclose = () => {
      onStatus("reconnecting");
      checkSession(); // a 401 here means the server was restarted
      setTimeout(open, delay);
      delay = Math.min(delay * 2, 10000);
    };
  }

  open();
}

function checkSession() {
  getHosts().catch(() => {}); // request() reports a 401 through the handler
}
