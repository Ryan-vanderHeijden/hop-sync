"""Fan-out of job events to WebSocket clients."""

import asyncio

from ..core.events import EntryFailed, EntrySkipped
from ..core.jobs import JobChanged, JobEntryEvent
from .serialize import job_to_json

QUEUE_SIZE = 1000


def event_to_message(event) -> dict | None:
    """JSON message for an event, or None if clients do not need it."""
    if isinstance(event, JobChanged):
        return {"type": "job", "job": job_to_json(event.job)}
    if isinstance(event, JobEntryEvent):
        inner = event.event
        if isinstance(inner, EntrySkipped):
            return {"type": "entry", "job_id": event.job_id, "kind": "skipped",
                    "path": inner.path, "reason": inner.reason}
        if isinstance(inner, EntryFailed):
            return {"type": "entry", "job_id": event.job_id, "kind": "failed",
                    "path": inner.path, "reason": inner.message}
    return None


class Subscriber:
    """One client's bounded message queue. None in the queue means 'close'."""

    def __init__(self) -> None:
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_SIZE)


class EventHub:
    def __init__(self) -> None:
        self._subscribers: set[Subscriber] = set()

    def subscribe(self) -> Subscriber:
        subscriber = Subscriber()
        self._subscribers.add(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: Subscriber) -> None:
        self._subscribers.discard(subscriber)

    def publish(self, event) -> None:
        """Sync on purpose, so it can be the JobManager's on_event callback."""
        message = event_to_message(event)
        if message is None:
            return
        for subscriber in list(self._subscribers):
            try:
                subscriber.queue.put_nowait(message)
            except asyncio.QueueFull:
                # A client that cannot keep up is cut off (never blocks the
                # transfers). It reconnects and gets a fresh snapshot.
                self._subscribers.discard(subscriber)
                while not subscriber.queue.empty():
                    subscriber.queue.get_nowait()
                subscriber.queue.put_nowait(None)
