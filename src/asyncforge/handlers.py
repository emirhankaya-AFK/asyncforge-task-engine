"""Explicit allowlist. Payload is data, never code, import paths or commands."""
import hashlib
import time


class RetryableTaskError(Exception):
    pass


class PermanentTaskError(Exception):
    pass


def validate_task(task_type: str, payload: dict) -> None:
    if task_type == "demo.sha256":
        if set(payload) != {"text"} or not isinstance(payload["text"], str) or len(payload["text"]) > 2048:
            raise ValueError("sha256 expects text up to 2048 characters")
    elif task_type == "demo.sleep":
        if set(payload) != {"seconds"} or type(payload["seconds"]) not in (int, float) or not 0 <= payload["seconds"] <= 10:
            raise ValueError("sleep expects seconds between 0 and 10")
    elif task_type == "demo.fail_n_times":
        if set(payload) != {"failures"} or type(payload["failures"]) is not int or not 0 <= payload["failures"] <= 9:
            raise ValueError("fail_n_times expects failures between 0 and 9")
    elif task_type == "demo.permanent_fail":
        if payload != {}:
            raise ValueError("permanent_fail takes empty payload")
    else:
        raise ValueError("unsupported task type")


def execute(task_type: str, payload: dict, attempt_no: int) -> dict:
    validate_task(task_type, payload)
    if task_type == "demo.sha256":
        return {"sha256": hashlib.sha256(payload["text"].encode()).hexdigest()}
    if task_type == "demo.sleep":
        time.sleep(payload["seconds"])
        return {"slept": payload["seconds"]}
    if task_type == "demo.fail_n_times":
        if attempt_no <= payload["failures"]:
            raise RetryableTaskError("synthetic transient failure")
        return {"completed_after": attempt_no}
    raise PermanentTaskError("synthetic permanent failure")
