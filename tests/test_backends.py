"""Tests for backend adapters — command building, output parsing, checkpoint."""

import json

import pytest

from agentd.protocol import EventType, ProgressType
from agentd.runtime.backends.claude import ClaudeAdapter
from agentd.runtime.backends.codex import CodexAdapter
from agentd.runtime.backends.pi import PiAdapter

@pytest.mark.parametrize("adapter", [PiAdapter(), ClaudeAdapter(), CodexAdapter()])
def test_malformed_json_raises_for_runner_warning(adapter):
    with pytest.raises(json.JSONDecodeError):
        adapter.parse_line("not json at all")


@pytest.mark.parametrize("adapter", [PiAdapter(), ClaudeAdapter(), CodexAdapter()])
def test_non_dict_json_raises_for_runner_warning(adapter):
    with pytest.raises(ValueError, match="backend output JSON must be an object"):
        adapter.parse_line('"just a string"')


# ---------------------------------------------------------------------------
# pi
# ---------------------------------------------------------------------------


class TestPi:
    def setup_method(self):
        self.adapter = PiAdapter()

    def test_name_and_capability(self):
        assert self.adapter.name == "pi"
        assert self.adapter.supports_steer is False

    def test_basic_command(self):
        cmd = self.adapter.build_command(
            prompt="hello",
            backend_args=[],
            checkpoint=None,
            cwd="/tmp",
        )
        assert cmd[0] == "pi"
        assert "--mode" in cmd
        assert "json" in cmd
        assert "-p" in cmd
        idx = cmd.index("-p")
        assert cmd[idx + 1] == "hello"

    def test_checkpoint_session_file_missing_raises(self, tmp_path, monkeypatch):
        from agentd.runtime.base import CheckpointLoadError

        monkeypatch.setenv("PI_CODING_AGENT_DIR", str(tmp_path))
        with pytest.raises(CheckpointLoadError):
            self.adapter.build_command(
                prompt="test",
                backend_args=[],
                checkpoint={
                    "session_id": "abc123",
                    "session_cwd": "/tmp/nonexistent",
                    "session_timestamp": "2026-01-01T00:00:00.000Z",
                },
                cwd="/tmp/nonexistent",
            )

    def test_checkpoint_no_session_id_is_noop(self):
        # Empty checkpoint (enabled but no data yet) should not raise
        cmd = self.adapter.build_command(
            prompt="test",
            backend_args=[],
            checkpoint={},
            cwd="/tmp",
        )
        assert cmd[0] == "pi"
        assert "--session" not in cmd

    def test_checkpoint_exact_path_restore(self, tmp_path, monkeypatch):
        """Rich checkpoint with cwd+timestamp constructs exact path."""
        monkeypatch.setenv("PI_CODING_AGENT_DIR", str(tmp_path))
        cwd = "/Users/testuser/project"
        session_id = "aaa-bbb-ccc"
        ts = "2026-03-07T17:56:18.243Z"
        ts_slug = "2026-03-07T17-56-18-243Z"

        # Create the session file at the expected path
        slug = cwd.replace("/", "-").strip("-")
        session_dir = tmp_path / "sessions" / f"--{slug}--"
        session_dir.mkdir(parents=True)
        session_file = session_dir / f"{ts_slug}_{session_id}.jsonl"
        session_file.write_text("{}")

        cmd = self.adapter.build_command(
            prompt="test",
            backend_args=[],
            checkpoint={
                "session_id": session_id,
                "session_cwd": cwd,
                "session_timestamp": ts,
            },
            cwd=cwd,
        )
        assert "--session" in cmd
        idx = cmd.index("--session")
        assert cmd[idx + 1] == str(session_file)

    def test_backend_args_passthrough(self):
        cmd = self.adapter.build_command(
            prompt="test",
            backend_args=["--model", "opus"],
            checkpoint=None,
            cwd="/tmp",
        )
        assert "--model" in cmd
        assert "opus" in cmd

    def test_parse_turn_end(self):
        line = json.dumps(
            {
                "type": "turn_end",
                "message": {
                    "role": "assistant",
                    "content": [{"type": "text", "text": "result text"}],
                },
            }
        )
        parsed = self.adapter.parse_line(line)
        # Spec §4: turn.result is internal; raw obj must not be public payload.
        # pi maps turn_end to an internal last_result update via ParsedLine.result.
        assert parsed.event_type == "log"
        assert parsed.payload == {}
        assert parsed.result == "result text"

    def test_parse_text_delta(self):
        line = json.dumps({"type": "text_delta", "text": "hello"})
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == EventType.TURN_PROGRESS
        assert parsed.payload["type"] == ProgressType.TEXT
        assert parsed.payload["content"] == "hello"

    def test_parse_thinking(self):
        line = json.dumps({"type": "thinking", "text": "let me think..."})
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == EventType.TURN_PROGRESS
        assert parsed.payload["type"] == ProgressType.THINKING

    def test_parse_tool_call(self):
        line = json.dumps(
            {
                "type": "toolcall_start",
                "name": "bash",
                "input": {"command": "ls"},
            }
        )
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == EventType.TURN_PROGRESS
        assert parsed.payload["type"] == ProgressType.TOOL_CALL
        assert parsed.payload["name"] == "bash"
        assert parsed.payload["status"] == "running"

    def test_parse_tool_end(self):
        line = json.dumps({"type": "toolcall_end", "name": "bash"})
        parsed = self.adapter.parse_line(line)
        assert parsed.payload["status"] == "completed"

    def test_parse_session(self):
        line = json.dumps({"type": "session", "id": "ses_abc123"})
        parsed = self.adapter.parse_line(line)
        assert parsed.checkpoint_update is not None
        assert parsed.checkpoint_update["session_id"] == "ses_abc123"

    def test_parse_session_rich(self):
        line = json.dumps(
            {
                "type": "session",
                "id": "ses_abc123",
                "cwd": "/home/user/project",
                "timestamp": "2026-03-07T17:56:18.243Z",
            }
        )
        parsed = self.adapter.parse_line(line)
        cp = parsed.checkpoint_update
        assert cp is not None
        assert cp["session_id"] == "ses_abc123"
        assert cp["session_cwd"] == "/home/user/project"
        assert cp["session_timestamp"] == "2026-03-07T17:56:18.243Z"

# ---------------------------------------------------------------------------
# claude
# ---------------------------------------------------------------------------


class TestClaude:
    def setup_method(self):
        self.adapter = ClaudeAdapter()

    def test_name_and_capability(self):
        assert self.adapter.name == "claude"
        assert self.adapter.supports_steer is False

    def test_basic_command(self):
        cmd = self.adapter.build_command(
            prompt="hello",
            backend_args=[],
            checkpoint=None,
            cwd="/tmp",
        )
        assert cmd[0] == "claude"
        assert "--output-format" in cmd
        assert "stream-json" in cmd
        assert "--permission-mode" in cmd
        assert "-p" in cmd

    def test_checkpoint_resume(self):
        cmd = self.adapter.build_command(
            prompt="test",
            backend_args=[],
            checkpoint={"session_id": "ses_xyz"},
            cwd="/tmp",
        )
        assert "--resume" in cmd
        idx = cmd.index("--resume")
        assert cmd[idx + 1] == "ses_xyz"

    def test_parse_result(self):
        line = json.dumps({"type": "result", "result": "claude says hi"})
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == EventType.TURN_END
        assert parsed.result == "claude says hi"

    def test_parse_text_content(self):
        line = json.dumps(
            {
                "type": "assistant",
                "message": {"content": [{"type": "text", "text": "output"}]},
            }
        )
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type in (EventType.TURN_PROGRESS, EventType.TURN_END)

    def test_parse_thinking(self):
        line = json.dumps(
            {
                "type": "assistant",
                "message": {"content": [{"type": "thinking", "thinking": "hmm..."}]},
            }
        )
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == EventType.TURN_PROGRESS

    def test_parse_tool_use(self):
        line = json.dumps(
            {
                "type": "assistant",
                "message": {
                    "content": [
                        {"type": "tool_use", "name": "read", "input": {"path": "f.py"}},
                    ],
                },
            }
        )
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == EventType.TURN_PROGRESS

    def test_parse_assistant_without_message_envelope_drops(self):
        # Spec §10: unmapped/malformed assistant content is dropped, not
        # passed through as raw payload.
        line = json.dumps(
            {"type": "assistant", "content": [{"type": "text", "text": "x"}]}
        )
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == "log"
        assert parsed.payload == {}

    def test_parse_user_tool_result_drops(self):
        # claude `user` events carry tool_result content (potentially MB-sized);
        # they must not be forwarded as public payload.
        line = json.dumps(
            {
                "type": "user",
                "message": {
                    "content": [{"type": "tool_result", "content": "x" * 100000}]
                },
            }
        )
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == "log"
        assert parsed.payload == {}

    def test_parse_unmapped_stream_event_drops(self):
        line = json.dumps(
            {
                "type": "stream_event",
                "event": {"content_type": "unknown_future_type", "data": "x"},
            }
        )
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == "log"
        assert parsed.payload == {}

    def test_parse_unmapped_top_level_event_drops(self):
        line = json.dumps({"type": "some_future_event", "data": "x"})
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == "log"
        assert parsed.payload == {}


class TestCodex:
    def setup_method(self):
        self.adapter = CodexAdapter()

    def test_name_and_capability(self):
        assert self.adapter.name == "codex"
        assert self.adapter.supports_steer is False

    def test_basic_command(self):
        cmd = self.adapter.build_command(
            prompt="hello",
            backend_args=[],
            checkpoint=None,
            cwd="/tmp",
        )
        assert cmd[0] == "codex"
        assert "exec" in cmd
        assert "--json" in cmd
        assert "--full-auto" in cmd
        assert "hello" in cmd

    def test_checkpoint_resume(self):
        cmd = self.adapter.build_command(
            prompt="test",
            backend_args=[],
            checkpoint={"thread_id": "th_abc"},
            cwd="/tmp",
        )
        assert "resume" in cmd
        assert "th_abc" in cmd

    def test_parse_agent_message(self):
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "codex says hi"},
            }
        )
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == EventType.TURN_END
        assert parsed.result == "codex says hi"

    def test_parse_command_execution(self):
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "command_execution",
                    "command": "ls -la",
                    "exit_code": 0,
                },
            }
        )
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == EventType.TURN_PROGRESS
        assert parsed.payload["type"] == ProgressType.TOOL_CALL
        assert parsed.payload["status"] == "completed"

    def test_parse_turn_completed(self):
        line = json.dumps({"type": "turn.completed"})
        parsed = self.adapter.parse_line(line)
        assert parsed.event_type == EventType.TURN_END

    def test_agent_message_then_turn_completed_keeps_result(self):
        # Real codex flow: item.completed[agent_message] sets last_result via
        # ParsedLine.result; subsequent turn.completed signals turn end without
        # overriding result. Both should be drop-safe public-wise.
        msg = self.adapter.parse_line(
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "final answer"},
                }
            )
        )
        assert msg.event_type == EventType.TURN_END
        assert msg.result == "final answer"
        assert msg.payload == {}
        end = self.adapter.parse_line(json.dumps({"type": "turn.completed"}))
        assert end.event_type == EventType.TURN_END
        # turn.completed has no result; runner keeps last_result from agent_message.
        assert end.result is None
        assert end.payload == {}

    def test_no_duplicate_subcommand(self):
        """If backend_args already has 'resume', don't add another."""
        cmd = self.adapter.build_command(
            prompt="test",
            backend_args=["resume", "th_old"],
            checkpoint={"thread_id": "th_new"},
            cwd="/tmp",
        )
        assert cmd.count("resume") == 1
