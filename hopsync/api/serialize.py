"""Turning job snapshots into JSON-ready dicts."""

from ..core.jobs import Job


def job_to_json(job: Job) -> dict:
    """The one place that decides what a job looks like to API clients."""
    return {
        "id": job.id,
        "src": {"host": job.src.host, "path": job.src.path},
        "dst": {"host": job.dst.host, "path": job.dst.path},
        "options": {
            "verify": job.options.verify,
            "links": job.options.links,
            "overwrite": job.options.overwrite,
        },
        "state": job.state.value,
        "bytes_done": job.bytes_done,
        "bytes_total": job.bytes_total,
        "bytes_transferred": job.bytes_transferred,
        "files_done": job.files_done,
        "files_found": job.files_found,
        "walk_complete": job.walk_complete,
        "current_file": job.current_file,
        "message": job.message,
        "error_kind": job.error_kind,
        "error_alias": job.error_alias,
        "attempt": job.attempt,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
    }
