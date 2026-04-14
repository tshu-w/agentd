from agentd.channels.telegram import format_failure_message


def test_format_failure_message_includes_typed_code_and_turn_id():
    message = format_failure_message(
        "BACKEND_NO_TERMINAL_EVENT",
        "turn_c7d515e5ac97",
    )

    assert message == "🔴 Failed: BACKEND_NO_TERMINAL_EVENT (turn: c7d515e5ac97)"


def test_format_failure_message_uses_unknown_fallbacks():
    assert format_failure_message("", "") == "🔴 Failed: UNKNOWN_ERROR (turn: unknown)"
