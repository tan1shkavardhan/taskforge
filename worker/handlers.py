import time


def handle_print(payload,heartbeat_callback=None):

    return {
        "output": payload
    }


def handle_sleep(payload, heartbeat_callback=None):

    seconds = payload.get("seconds", 1)

    if not isinstance(seconds, (int, float)):
        raise ValueError(
            "seconds must be numeric"
        )

    remaining = seconds

    while remaining > 0:

        sleep_time = min(5, remaining)

        time.sleep(sleep_time)

        remaining -= sleep_time

        if heartbeat_callback:
            heartbeat_callback()

    return {
        "slept_for": seconds
    }


TASK_HANDLERS = {
    "print": handle_print,
    "sleep": handle_sleep
}