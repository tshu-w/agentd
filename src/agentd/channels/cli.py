"""CLI chat channel adapter for agentd.

A minimal interactive REPL that sends user input to an agentd root actor
and prints progress + results to the terminal.

    python -m agentd.channels.cli [--name NAME]
"""

import argparse
import asyncio
import contextlib
import sys

from agentd.channels.lib import (
    WaitResult,
    agentd_status,
    agentd_stop,
    notify,
    wait_for_actor,
)

TASK_NAME = "cli:chat"


async def run_turn(text: str, actor_name: str) -> tuple[str, WaitResult]:
    result = notify(
        actor_name,
        event_type="message",
        payload={"text": text},
    )
    actor_id = result["actor_id"]

    wait = await wait_for_actor(
        actor_id,
        on_progress=lambda t: print(f"\r  {t}", end="", flush=True),
    )
    print()
    return actor_id, wait


async def main() -> None:
    parser = argparse.ArgumentParser(description="CLI chat adapter for agentd")
    parser.add_argument("--name", default=TASK_NAME, help="actor name")
    args = parser.parse_args()

    try:
        agentd_status()
    except Exception as e:
        print(f"agentd not ready: {e}", file=sys.stderr)
        sys.exit(1)

    actor_name = args.name
    print(f"agentd CLI chat ({actor_name}). Type /new for new session, /quit to exit.\n")

    while True:
        try:
            text = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not text:
            continue
        if text == "/quit":
            break
        if text == "/new":
            with contextlib.suppress(Exception):
                agentd_stop(actor_name)
            print("🆕 New session.\n")
            continue

        actor_id, result = await run_turn(text, actor_name)

        if result.stopped:
            print("🛑 Stopped.\n")
        elif not result.ok:
            print(f"🔴 Error: {result.error}\n")
        elif result.result_text:
            print(f"\nagent> {result.result_text}\n")
        else:
            print()


if __name__ == "__main__":
    asyncio.run(main())
