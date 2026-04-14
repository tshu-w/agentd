from agentd.channels.lib import ProgressState


def test_progress_state_does_not_carry_tool_detail_into_text_phase():
    ps = ProgressState()

    tool_text = ps.update(
        {
            "event_type": "turn.progress",
            "payload": {
                "type": "tool_call",
                "name": "bash",
                "args": {"command": "echo hello"},
                "status": "running",
            },
        }
    )
    assert tool_text == "✨ Running tool…\nStep 1: $ echo hello"

    text_phase = ps.update(
        {
            "event_type": "turn.progress",
            "payload": {
                "type": "text",
                "content": "hi",
            },
        }
    )
    assert text_phase == "✨ Generating reply…"
