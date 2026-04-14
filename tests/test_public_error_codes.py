from agentd.protocol import PublicErrorCode, TurnOutcome
from agentd.scheduler.scheduler import _public_error_code


def test_public_error_code_classifies_no_terminal_event():
    assert (
        _public_error_code(
            {
                "outcome": TurnOutcome.FAILED.value,
                "error": "no turn.end received",
            }
        )
        == PublicErrorCode.BACKEND_NO_TERMINAL_EVENT.value
    )


def test_public_error_code_classifies_nonzero_exit():
    assert (
        _public_error_code(
            {
                "outcome": TurnOutcome.FAILED.value,
                "error": "exit code 1: boom",
            }
        )
        == PublicErrorCode.BACKEND_EXIT_NONZERO.value
    )


def test_public_error_code_classifies_timeout():
    assert (
        _public_error_code(
            {
                "outcome": TurnOutcome.FAILED.value,
                "error": "turn deadline exceeded (1800s)",
            }
        )
        == PublicErrorCode.BACKEND_TIMEOUT.value
    )


def test_public_error_code_classifies_stopped_turn():
    assert (
        _public_error_code(
            {
                "outcome": TurnOutcome.INTERRUPTED.value,
                "error": None,
            }
        )
        == PublicErrorCode.ACTOR_STOPPED.value
    )
