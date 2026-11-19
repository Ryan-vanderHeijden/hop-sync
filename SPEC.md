# hop-sync: dual-pane SFTP file mover (HPC <-> home server, relayed through my computer)

## Goal
A lightweight local GUI with two directory panes (HPC on one side, home server on the other). I drag-and-drop or copy/paste files and folders between panes. Behind the scenes, data streams HPC -> my computer (in memory only) -> home server. Nothing is written to my local disk.

This is a learning project. Prefer clear, readable code over clever code, and explain design decisions briefly as you go.

## Situation
- Three machines: my personal computer (runs this app), the home server (`homeserver`), and an HPC cluster.
- My computer can SSH to both. The HPC and the home server cannot reach each other.
- Development happens on Linux; the app runs on Windows. Code must work on both. Do not depend on OpenSSH `ControlMaster`; hold sessions inside the app process.
- Remote paths are always POSIX: build them with `posixpath`, never `os.path` (on Windows `os.path` would join HPC paths with backslashes). Use `os.path`/`pathlib` only for local files like `~/.ssh/config`.
- Home server auth: SSH key.
- HPC auth: SSH key AND password (no MFA, no Duo). Prompt for the password once at startup in the terminal (`getpass`) and keep it in memory only.
- It is unknown whether the HPC asks for the password via the `password` method or `keyboard-interactive`. Support both: pass the password to asyncssh's `password=` and also answer keyboard-interactive prompts with it. (To check: `ssh -v <hpc-alias> 2>&1 | grep -i "can continue"` lists the methods the server offers.)
- Connection details should come from `~/.ssh/config` (aliases, keys, ProxyJump) so I never retype hostnames.
- Key passphrases: use an SSH agent if one is running (Pageant or the Windows OpenSSH agent on Windows, `SSH_AUTH_SOCK` on Linux). Otherwise prompt for the passphrase with `getpass` at startup and keep it in memory only.
- Out of scope: keeping the computer awake. If it sleeps mid-transfer, the connection drops and the job resumes from its `.hopsync-part` file after reconnect.

## Stack
- Python 3.11+
- `asyncssh` for SSH/SFTP (one persistent connection per host, SFTP channels multiplexed over it)
- FastAPI + uvicorn backend, WebSocket for progress events
- Plain HTML/CSS/JS front end (no build step, no framework)
- `pytest` + `pytest-asyncio` for tests

## Architecture
```
hopsync/
  core/        transfer engine + SSH session management (NO web or UI imports)
  api/         FastAPI app: REST endpoints + WebSocket
  web/         static front end (index.html, app.js, style.css)
  cli.py       thin CLI over core (milestone 1)
tests/
```
Key rule: `core` emits progress as events (callback or asyncio queue). The CLI, the web API, and the tests all consume the same events.

## Milestones (do them in order; each must work and be tested before the next)

### 1. CLI core
- Connect to both hosts (key, plus password for the HPC; masked prompt, via `getpass`).
- `list <host>:<path>` prints a directory using SFTP (not parsed `ls`).
- `copy <srchost>:<path> <dsthost>:<path>` streams one file with a progress bar.
- Pipeline: a reader task fills a bounded `asyncio.Queue` of 1-4 MB chunks, a writer task drains it. This gives backpressure and keeps both links busy.
- Memory budget: queue depth 4 × chunk size 4 MB = at most 16 MB buffered per job.
- Throughput: confirm asyncssh keeps several SFTP requests in flight per read/write (`block_size` / `max_requests`). Sequential single requests are limited by network latency. Measure throughput and record it.
- Destination path rules work like `cp`: if the destination is an existing directory, copy into it under the source name; otherwise the destination is the new file name. The parent directory must exist.
- Never change existing destination data by default. An existing destination file that differs (by size or mtime) is refused with a clear error and left untouched; identical files are skipped as "already copied"; `--overwrite` allows replacement. Existing destination directories keep their mode and mtime (hopsync only sets metadata on directories it created). Milestone 6's conflict dialog later replaces the hard refusal.
- Copying between two paths on the same host is refused with a clear error. Relaying through this computer would be pointless.
- Acceptance: a generated 1 GB file copies with correct size, and process RSS grows by less than 50 MB over its pre-copy baseline (buffer budget is 16 MB).

### 2a. Resume and verify
- Write to a temporary `<name>.hopsync-part` file and rename it to the final name only after verification passes. A half-written file must never look complete.
- Resume by byte offset if a `.hopsync-part` file exists, but only if the source's size and mtime still match what was recorded when the transfer started. Otherwise start over.
- Cancel keeps the `.hopsync-part` file so the job can be resumed; retry resumes from it.
- Verify after transfer: size check always. Optional hash check: compute sha256 in Python as chunks pass through memory, then compare against one remote `sha256sum` on the destination (avoids hashing twice remotely).

### 2b. Recursive copy
- Recursive directory copy: walk the tree, transfer file by file, show per-file progress.
- A directory copy is ONE job containing child files. Its total byte count grows while the walk runs (HPC listings are slow, so do not walk everything before starting) and is final once the walk finishes.
- Preserve mtime and permission bits (not ownership, not setuid/setgid).
- Symlinks: skip by default, with a `--links` flag to copy them as links.
- File names that are not valid UTF-8 are skipped with a clear warning naming the parent directory.

### 3. Transfer queue
- Job model: id, source, dest, state (queued/running/done/failed/cancelled), bytes done/total. (No pause: cancel plus resume-on-retry covers it.)
- Run N jobs concurrently (default 2). Support cancel and retry. Retry with backoff on dropped connections.
- Stay under sshd's `MaxSessions` (often 10 channels per connection): reuse one SFTP channel per host shared by all jobs, and cap the extra exec channels (e.g. for `sha256sum`) at 2 per host.
- Keepalives (`keepalive_interval=30`) and silent reconnect using the in-memory password.
- Never retry authentication failures. Retry only network-level drops, with a capped number of attempts. A wrong or changed password must fail once and surface an error, not loop (repeated failed logins can lock the HPC account).

### 4. HTTP + WebSocket API
- `GET /api/list?host=&path=`, `POST /api/jobs`, `DELETE /api/jobs/{id}`, `GET /api/jobs`.
- `WS /api/events` pushes job progress and state changes.
- Bind to `127.0.0.1` ONLY. Generate a random token at startup and open the browser with it once; the server exchanges it for an `HttpOnly; SameSite=Strict` session cookie, which is then required on every request and on the WebSocket (browsers can't set custom headers on WebSockets, so a cookie is the simplest option).
- Check the `Origin` header on the WebSocket (CORS does not protect WebSockets) and the `Host` header on every request (`127.0.0.1`/`localhost` only, to block DNS rebinding).
- Disable or redact uvicorn access logs so the token never lands in a log.
- Password entry happens ONLY via the terminal `getpass` prompt at startup, never through the browser or HTTP. It is never logged, never written to disk, and never returned by any endpoint.

### 5. Dual-pane UI
- Two panes, each with a host picker, a path bar, and a file list (name, size, mtime). Lazy-load directories; HPC filesystems can be slow to list.
- Drag a file/folder from one pane onto the other to enqueue a job. Also support select + Ctrl+C / Ctrl+V between panes.
- Progress panel showing jobs with speed, ETA, cancel, and retry.

### 6. Polish
- Conflict handling dialog: overwrite / skip / rename (apply to all).
- Clear error messages (permission denied, disk full, connection lost).
- Optional `--tar` mode for directories with huge numbers of small files: `tar cf - -C <parent> <name>` on the source piped to `tar xf - -C <destdir>` on the destination. No compression (it mostly costs login-node CPU). Every path argument goes through `shlex.quote`. This mode has no resume and only shows bytes streamed so far, with no total.
- Packaging: `pipx`-installable, single `hopsync` command that starts the server and opens the browser.
- README with setup and usage.

## Security requirements
- Never persist the HPC password or key passphrase. Memory only.
- Local server on `127.0.0.1` with a startup token. No CORS wildcard.
- Validate and normalize all paths from the client. Do not let the API run arbitrary shell commands; the only remote commands allowed are fixed ones like `sha256sum` with properly quoted arguments.
- Do not disable host key checking. Use `known_hosts`.

## Testing
- Run a local `sshd` in a container (or two, to stand in for HPC and server) so tests never touch the real machines. Use a password+key setup in one of them to mimic the HPC.
- Test: large-file copy, resume after killed connection, directory copy, cancel mid-transfer, reconnect after drop, wrong password fails once without retrying.
- Simulate a dropped connection by aborting the asyncssh connection from inside the test (no extra proxy tools).
- Large-file tests use a generated 1 GB file and are marked `@pytest.mark.slow` so the quick test run skips them.
- Keep a small `docker-compose.yml` for the test hosts. Prerequisite: Docker (Docker Desktop on Windows).

## How I want to work with you (Claude Code)
- I am not a strong programmer, so explain non-obvious choices in a sentence or two and keep functions small.
- Work one milestone at a time. At the end of each, show me how to run it and what you tested.
- Ask before adding heavy dependencies or changing the architecture.
- Commit after each working milestone with a clear message. (The project needs `git init` before the first commit.)
