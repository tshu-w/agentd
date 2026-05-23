import asyncio
from pathlib import Path

import pytest

from agentd.api.server import Daemon
from agentd.config import AgentDConfig
from agentd.protocol import AGENTD_FRAME_MAX


class _FakeServer:
    def close(self) -> None:
        pass

    async def wait_closed(self) -> None:
        pass


@pytest.mark.asyncio
async def test_daemon_unix_server_uses_agentd_frame_limit(tmp_path, monkeypatch):
    cfg = AgentDConfig(workspace=str(tmp_path))
    daemon = Daemon(cfg)
    captured = {}

    async def fake_start_unix_server(handler, *, path, limit):
        captured["handler"] = handler
        captured["path"] = path
        captured["limit"] = limit
        Path(path).touch()
        daemon._shutdown_event.set()
        return _FakeServer()

    monkeypatch.setattr(asyncio, "start_unix_server", fake_start_unix_server)

    await daemon.run()

    assert captured["handler"] == daemon._handle_connection
    assert captured["limit"] == AGENTD_FRAME_MAX
