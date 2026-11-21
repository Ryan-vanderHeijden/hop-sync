[![tests](https://github.com/Ryan-vanderHeijden/hop-sync/actions/workflows/tests.yml/badge.svg)](https://github.com/Ryan-vanderHeijden/hop-sync/actions/workflows/tests.yml)

# hop-sync

hop-sync moves files and folders between two machines that cannot reach each
other, for example an HPC cluster and your home server. Your own computer sits
in the middle: the data streams from one machine to the other through your
computer's memory. Nothing is written to your local disk.

```
   HPC  ──────►  your PC (memory only)  ──────►  home server
```

You use it from a local web page with two panes (drag a file from one pane to
the other), or from the command line.

## Requirements

- Python 3.11 or newer. If you use `uv` (below) it can install Python for you.
- [uv](https://docs.astral.sh/uv/) (recommended) or [pipx](https://pipx.pypa.io/).
- An OpenSSH setup: your hosts are listed in `~/.ssh/config`, and their host
  keys are in `~/.ssh/known_hosts` (see "SSH setup" below). On Windows that is
  `C:\Users\<you>\.ssh\`.

## Install

From the project folder (or from a git URL), in PowerShell:

```powershell
uv tool install C:\path\to\hop-sync
# or: uv tool install git+https://<your-git-host>/<you>/hop-sync.git
hopsync --version
```

If `hopsync` is not found afterwards, run `uv tool update-shell` and open a new
PowerShell window.

With pipx instead: `pipx install C:\path\to\hop-sync`.

**Updating** after you changed or pulled the code:

```powershell
uv tool install --reinstall C:\path\to\hop-sync
```

A tool install is a separate copy of the program. You can rename or move the
source folder afterwards and `hopsync` keeps working. (A development setup with
a `.venv` inside the folder does NOT survive a rename, see Troubleshooting.)

## SSH setup checklist

hop-sync reads your normal OpenSSH config, so you never type host names twice.

1. Every machine has a `Host` entry in `~/.ssh/config`, with `HostName`, `User`
   and `IdentityFile` (and `ProxyJump` if you need one). Example:

   ```
   Host homeserver
       HostName 192.168.1.20
       User me
       IdentityFile ~/.ssh/id_ed25519

   Host <hpc-alias>
       HostName login.cluster.example.org
       User me
       IdentityFile ~/.ssh/id_ed25519
   ```

2. Connect once with plain `ssh <alias>` for each host and accept the host key.
   hop-sync never turns host key checking off: an unknown host key is refused.
3. Key passphrases: if you use an SSH agent (the Windows OpenSSH agent, Pageant,
   or `SSH_AUTH_SOCK` on Linux), hop-sync uses it. Without an agent, hop-sync
   asks for the passphrase when it is needed.
4. Hosts that need a password in addition to the key (typical for HPC logins)
   must be named with `--password-for <alias>`, or listed under `password_for`
   in the settings file. hop-sync asks for that password once, when it starts,
   and keeps it in memory only.

## Settings file

Optional. Find (and create, if it is missing) the file with:

```powershell
hopsync settings
```

It prints the path: `%APPDATA%\hopsync\settings.toml` on Windows, and
`~/.config/hopsync/settings.toml` elsewhere. The new file is a commented
template. Example:

```toml
hosts = ["<hpc-alias>", "homeserver"] # what the host picker shows, in this order
password_for = ["<hpc-alias>"]        # hosts that need a password
ssh_config = "C:/Users/you/.ssh/config"
jobs = 2                              # copies running at once
port = 0                              # 0 = pick a free port
```

- All keys are optional. Command-line options win over the file, and
  `--password-for` on the command line adds to `password_for`.
- `hosts` is the answer to "why does the picker list `github.com` and old
  machines?": without it, every `Host` in your ssh config is shown. With it,
  only these are shown and allowed, and the first two become the default panes.
  A name that is not in your ssh config is dropped with a warning.
- **Never put passwords, passphrases or tokens in this file.** A key that looks
  like a secret makes hop-sync refuse to start. Passwords are asked for at
  startup instead.

## Using the web UI

```powershell
hopsync
```

(`hopsync` alone is the same as `hopsync serve`. Add `--password-for <alias>`
for hosts that need a password if you did not put them in the settings file.)
It prompts for the passwords, prints a link and opens your browser. The link
contains a one-time-per-run token; opening it signs your browser in. Press
Ctrl+C in the terminal to stop.

- **Panes.** Each pane has a host picker, a path bar (type a path and press
  Enter), Up and Refresh. Directories are only listed when you open them.
  Double-click or Enter opens a folder, Backspace goes up.
- **Selecting.** Click, Ctrl+click to add or remove, Shift+click for a range,
  arrow keys to move, Ctrl+A for all.
- **Copying.** Drag items onto the other pane (drop on a folder to copy into
  it), or select and press Ctrl+C, click into the other pane, press Ctrl+V.
  Each item becomes one job. Both panes on the same host is refused: there
  is nothing to relay.
- **Progress panel** (bottom). One row per job with a progress bar, speed and
  ETA, the current file, skipped and failed counts (click "failed" to see why),
  and a **Cancel** button (queued or running) or **Retry** button (failed or
  cancelled). Retry and re-dropping continue where the copy stopped. "Clear
  finished" hides completed jobs. The panel can be collapsed.
- **Top bar options.** "Verify (sha256)" compares checksums after copying.
  "Copy symlinks as links" copies links instead of skipping them.
  **"If a file exists"** chooses what happens when a different file is already
  at the destination: *Ask* (a dialog appears), *Skip*, *Keep both* (the new file
  gets `name (1).ext`) or *Overwrite* (shown in warning colours). It always
  starts as *Ask* and is never remembered.
- **The dialog** shows the existing and the new file side by side. Buttons:
  Overwrite, Skip (the default), Keep both; a checkbox applies your answer to
  the rest of that job. Escape closes the dialog without deciding: the job keeps
  waiting, and a "1 decision waiting" badge on the job reopens it. A job that is
  waiting still occupies one of the concurrent slots.

## Using the command line

```powershell
hopsync list <hpc-alias>:~/data
hopsync --password-for <hpc-alias> copy <hpc-alias>:data/big.tar homeserver:backups
hopsync --password-for <hpc-alias> copy <hpc-alias>:data/a.bin <hpc-alias>:data/b.bin homeserver:backups
```

- Paths are `alias:path`. `~/` or no leading slash means your home directory
  on that host. Options like `--config` and `--password-for` go before the
  command word (`list`, `copy`, `serve`).
- `copy` takes one or more sources, then the destination. Like `cp`, an
  existing destination folder receives the items inside it, and with several
  sources the destination must be an existing folder. Folders are copied
  recursively.
- `--jobs N` copies N things at once (default 2). `--verify` also compares
  sha256 checksums. `--links` copies symlinks as links.
- `--on-conflict refuse|skip|rename|overwrite` chooses what to do when a
  different file already exists (default `refuse`, which reports it and
  exits with status 1). `--overwrite` means `--on-conflict overwrite`.
- Ctrl+C cancels the jobs; partial files are kept so you can run the same
  command again to resume.

## How your data is kept safe

- **Nothing is overwritten by default.** If a file already exists with a
  different size or modified time, it is left alone and reported. A file with
  the same size and time is "already copied" and skipped, so rerunning a copy
  only does the missing work. Overwriting needs `--overwrite` or the
  "Overwrite" choice (or your answer in the dialog).
- **Existing folders are left as they were.** hop-sync only sets permissions
  and times on folders it created itself (including, when you rerun an
  interrupted folder copy, the folders an earlier run created: the marker
  below tells hop-sync they are its own).
- **A half-written file never looks complete.** Data goes to
  `<name>.hopsync-part`, with a small `<name>.hopsync-part.json` next to it
  that records which source it came from. Only after the size (and checksum,
  with verify) matches is it renamed to the real name. If a copy is interrupted
  (cancel, closed laptop, network drop), run it again: it resumes from the
  partial file, but only if the source still has the same size and modified time.
- **Unfinished folder copies** leave a `.hopsync-incomplete.json` file in the
  top folder hop-sync created. Running the same copy again continues in that
  folder instead of creating a second nested one. The marker is deleted when
  the copy finishes cleanly. It is never written into a folder that already
  existed.
- **Files that appear during a copy** are never replaced: the final rename
  refuses to overwrite.
- **Verify.** The size is always checked. With `--verify` (or the checkbox) a
  sha256 is computed while the data streams through, and compared with
  `sha256sum` run on the destination. A mismatch deletes the partial file.
- Modified times and permission bits are copied (not owners, not setuid).
  Names that are not valid UTF-8 are skipped with a warning.

## Security model

- The web server listens on `127.0.0.1` only (there is no option to change that).
  Other computers cannot reach it.
- At startup hop-sync makes a random token and prints a link with it. Opening
  the link gives your browser a session cookie (HttpOnly, SameSite=Strict);
  every other request needs that cookie. The token is not written to any log.
- Every request must use a `127.0.0.1` or `localhost` Host header (this blocks
  DNS rebinding), and the WebSocket (progress updates) must come from the same
  origin. There are no CORS headers. Pages also carry a strict Content Security
  Policy, and file names from servers are never treated as HTML.
- Passwords and key passphrases are typed into the terminal, kept in memory
  only, and never logged, written to disk or sent to the browser.
- A failed login is never retried: a wrong or changed password fails once, and
  hop-sync will not try that host again until you restart it. Repeated bad
  logins can lock an HPC account.
- The program only runs fixed commands on your servers (`sha256sum` with a
  quoted path). Host key checking stays on.

## Troubleshooting

- **"host key for 'x' is not trusted".** Run `ssh <alias>` once and accept the
  key, or fix the entry in `~/.ssh/known_hosts` if the machine was reinstalled.
- **"authentication ... failed; restart hopsync with --password-for <alias>".**
  The login was refused, or the host needs a password. Start with
  `--password-for <alias>` (or add it to `password_for`) and type the right
  password. After one failure hop-sync does not retry that host.
- **"uv trampoline failed to canonicalize script path"** after renaming or
  moving the project folder. Scripts in `.venv\Scripts` remember the old path.
  Delete `.venv` and recreate it (`uv venv .venv` then
  `uv pip install -e ".[dev]"`), or use the tool install above, which does not
  depend on the folder.
- **Transfers are slow.** A copy runs at the speed of the slower of the two
  links (download from one host, upload to the other). VPNs can cap speed. So
  can Tailscale when it reaches a home machine through your router's public
  address instead of the local network (`tailscale ping <host>` shows `via
  <public-ip>:41641` instead of `via 192.168.x.x:41641`); another active VPN
  often causes this. An extra ssh alias with the machine's local IP avoids it.
  Test each link on its own:

  ```powershell
  Measure-Command { cmd /c "ssh <alias> head -c 500000000 /dev/zero > NUL" }
  ```

  500 MB divided by the seconds shown is the speed of that link.
- **"destination ... is full".** The disk (or quota) on the destination is out
  of space. The partial file is kept: free some space and run the copy again to
  resume. Single files are checked before starting; for folders you see the
  message on the file that did not fit.
- **A host you do not want in the picker** (`github.com`, an old machine): set
  `hosts` in the settings file.
- **"settings.toml must not contain secrets".** Remove the password,
  passphrase, secret or token key from the file.

## For developers

Set up and run the tests (PowerShell shown; on Linux use `.venv/bin/...`):

```powershell
uv venv .venv
uv pip install -e ".[dev]"
.venv\Scripts\pytest                  # fast tests, plus Docker-based ones when Docker is running
.venv\Scripts\pytest -m slow          # 1 GiB transfer test and the wheel build check
node --test tests/web/format.test.mjs tests/web/ui.test.mjs
```

The integration tests start two throwaway `sshd` containers (`server`: key
only, `hpc`: key and password) from `tests/docker-compose.yml`, bound to
127.0.0.1, with a generated key in `tests/.hostdata/`. Without Docker they are
skipped. Pass the file names to `node --test` explicitly (a directory argument
does not work on every Node version).

### Continuous integration

`.github/workflows/tests.yml` runs on every push and pull request:

- **linux**: Python 3.11 and 3.13 on Ubuntu. Runs the whole default test run,
  including the Docker-based integration tests, plus the front end tests.
- **windows**: Python 3.13. Runs `pytest -m "not integration and not slow"`,
  because the Windows runner cannot run the Linux sshd containers. Any new
  test that needs Docker must carry the `integration` marker, and anything
  Linux-only needs a skip marker, or this job fails.
- **slow**: the 1 GiB transfer test and the wheel build. Not run on pushes; start
  it by hand from the Actions tab (Run workflow).

Layout:

```
hopsync/
  core/       transfer engine, SSH sessions, job queue, conflict rules (no UI imports)
  api/        FastAPI app: REST, WebSocket, security checks
  web/        plain HTML/CSS/JS front end (no build step)
  cli.py      command line
  settings.py settings.toml (outside core on purpose)
tests/
```

The rule: `core` never imports from `api`, `web`, `cli` or `settings`, never
prints and never prompts. It receives plain arguments (including secrets) and
reports through events. The CLI and the web API both consume the same events.
The version lives in `hopsync/__init__.py` (`__version__`); `pyproject.toml`
reads it from there.
