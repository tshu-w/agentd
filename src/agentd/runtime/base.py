"""Backend adapter contract.

Each backend adapter normalizes its CLI tool's behavior into
canonical signals that the Runtime understands.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from agentd.protocol import ParsedLine


class CheckpointLoadError(Exception):
    """Raised when a checkpoint exists but cannot be loaded."""


class BackendAdapter(ABC):
    """Base class for backend adapters."""

    name: str
    supports_steer: bool = False

    @abstractmethod
    def build_command(
        self,
        *,
        prompt: str,
        backend_args: list[str],
        checkpoint: dict[str, Any] | None,
        cwd: str | None,
    ) -> list[str]:
        """Build the shell command to execute for this turn."""
        ...

    @abstractmethod
    def parse_line(self, line: str) -> ParsedLine:
        """Parse a single stdout line into a normalized event."""
        ...

    def checkpoint_event_type(self, loaded: bool, has_data: bool) -> str:
        """Return the appropriate checkpoint event type."""
        if not loaded:
            return "actor.checkpoint.missed"
        return "actor.checkpoint.loaded"
