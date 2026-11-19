"""Turning job snapshots into JSON-ready dicts."""

from ..core.jobs import Job


def pending_conflict_to_json(conflict) -> dict | None:
    if conflict is None:
        return None
    return {
        "conflict_id": conflict.conflict_id,
        "job_id": conflict.job_id,
        "path": conflict.path,
        "src_size": conflict.src_size,
        "src_mtime": conflict.src_mtime,
        "dst_size": conflict.dst_size,
        "dst_mtime": conflict.dst_mtime,
    }


def job_to_json(job: Job) -> dict:
    """The one place that decides what a job looks like to API clients."""
    return {
        "id": job.id,
        "src": {"host": job.src.host, "path": job.src.path},
        "dst": {"host": job.dst.host, "path": job.dst.path},
        "options": {
            "verify": job.options.verify,
            "links": job.options.links,
            "on_conflict": job.options.on_conflict,
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
        "pending_conflict": pending_conflict_to_json(job.pending_conflict),
        "attempt": job.attempt,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
    }
