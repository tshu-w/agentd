"""In-memory event pub/sub with per-subscriber queues.

Subscribers receive events as dicts. Slow consumers are disconnected.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

logger = logging.getLogger(__name__)

MAX_QUEUE_SIZE = 256


class SlowConsumerError(Exception):
    pass


class Subscription:
    """Async iterator over events for one subscriber."""

    def __init__(
        self,
        *,
        actor_id: str | None = None,
        since_seq: int = 0,
        max_size: int = MAX_QUEUE_SIZE,
    ):
        self.actor_id = actor_id
        self.since_seq = since_seq
        # +1 capacity reserved for the close sentinel
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=max_size + 1)
        self._closed = False
        self._overflow = False

    def _deliver(self, event: dict[str, Any]) -> None:
        if self._closed:
            return
        # Filter by actor_id if set
        if self.actor_id and event.get("actor_id") != self.actor_id:
            return
        # Filter by seq
        seq = event.get("seq", 0)
        if seq <= self.since_seq:
            return
        # Reserve last slot for close sentinel
        if self._queue.qsize() >= self._queue.maxsize - 1:
            self._overflow = True
            self._closed = True
            self._queue.put_nowait(None)  # guaranteed: last slot reserved
            return
        self._queue.put_nowait(event)

    def __aiter__(self):
        return self

    async def __anext__(self) -> dict[str, Any]:
        item = await self._queue.get()
        if item is None:
            if self._overflow:
                raise SlowConsumerError("subscriber queue overflow")
            raise StopAsyncIteration
        return item

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait(None)


class EventBus:
    """Publish events to multiple async subscribers."""

    def __init__(self) -> None:
        self._subscribers: list[Subscription] = []
        self._lock = asyncio.Lock()

    async def subscribe(
        self,
        *,
        actor_id: str | None = None,
        since_seq: int = 0,
    ) -> Subscription:
        sub = Subscription(actor_id=actor_id, since_seq=since_seq)
        async with self._lock:
            self._subscribers.append(sub)
        return sub

    async def unsubscribe(self, sub: Subscription) -> None:
        sub.close()
        async with self._lock:
            with contextlib.suppress(ValueError):
                self._subscribers.remove(sub)

    def publish(self, event: dict[str, Any]) -> None:
        """Publish event to all active subscribers (non-blocking)."""
        dead: list[Subscription] = []
        for sub in self._subscribers:
            if sub._closed:
                dead.append(sub)
                continue
            sub._deliver(event)
        # Cleanup dead subscribers
        if dead:
            for d in dead:
                with contextlib.suppress(ValueError):
                    self._subscribers.remove(d)

    async def close(self) -> None:
        async with self._lock:
            for sub in self._subscribers:
                sub.close()
            self._subscribers.clear()
