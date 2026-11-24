"""The settings of the streaming job, from the environment that its Compose service sets.

Every figure that differs between hosts comes from the environment, so the job types no address, path or limit. The late limit and the cutoff step are
in seconds in the environment and in microseconds in the job. ``STREAM_TEST_EXIT_AFTER_SINK`` is a test input in the way ``STACK_TEST_AVAILABLE_MIB``
is: a whole number N from 1 that stops the process right after the N-th sink commit of the process (one merge into one lake table), so that a restart
test can stop the job after its first sink commit. It is off unless set.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass

from replay.events.formats import FEATURE_READY_TOPIC, INPUT_TOPIC
from spark.streaming.model import MINUTE, SECOND

DEFAULT_LATE_LIMIT_SECONDS = 30.0
DEFAULT_TRIGGER_SECONDS = 2.0
DEFAULT_MAX_OFFSETS = 50_000
DEFAULT_BRANCH = "stream"
MAXIMUM_SECONDS = 31_536_000.0
MAXIMUM_OFFSETS = 2_147_483_647.0


class StreamSettingsError(ValueError):
    """A setting is missing or cannot be used. The message names it and never holds a value that could be a secret."""


@dataclass(frozen=True, slots=True)
class StreamSettings:
    bootstrap: str
    registry_url: str
    checkpoint_dir: str
    input_topic: str = INPUT_TOPIC
    output_topic: str = FEATURE_READY_TOPIC
    branch: str = DEFAULT_BRANCH
    late_limit_us: int = int(DEFAULT_LATE_LIMIT_SECONDS * SECOND)
    cutoff_step_us: int = MINUTE
    trigger_seconds: float = DEFAULT_TRIGGER_SECONDS
    max_offsets_per_trigger: int = DEFAULT_MAX_OFFSETS
    max_seconds: float = 0.0
    clinical_file: str = ""
    test_exit_after_sink: int | None = None


def required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "")
    if not value:
        raise StreamSettingsError(f"the environment variable {name} is not set")
    return value


def number(env: Mapping[str, str], name: str, default: float, *, minimum: float, allow_zero: bool = False, maximum: float = MAXIMUM_SECONDS) -> float:
    """The number in ``name``, or ``default`` when it is not set: finite, at least ``minimum`` (more than zero unless ``allow_zero``) and at most ``maximum``
    (a year in seconds unless given). Any other value is a ``StreamSettingsError`` that names the variable and not the value."""
    text = env.get(name, "")
    if not text:
        return default
    try:
        value = float(text)
    except ValueError:
        raise StreamSettingsError(f"{name} is not a number") from None
    if not math.isfinite(value) or value < minimum or (value == 0 and not allow_zero):
        raise StreamSettingsError(f"{name} must be {'zero or more' if allow_zero else 'more than zero'}")
    if value > maximum:
        raise StreamSettingsError(f"{name} must be at most {maximum:g}")
    return value


def offset_count(env: Mapping[str, str], name: str, default: int) -> int:
    """The number of offsets per trigger: a whole number from 1 to ``MAXIMUM_OFFSETS``. A fraction is refused and never cut to a whole number."""
    value = number(env, name, float(default), minimum=0, maximum=MAXIMUM_OFFSETS)
    if value < 1 or not value.is_integer():
        raise StreamSettingsError(f"{name} must be a whole number from 1")
    return int(value)


def from_environment(env: Mapping[str, str]) -> StreamSettings:
    test_exit: int | None = None
    if env.get("STREAM_TEST_EXIT_AFTER_SINK", ""):
        text = env["STREAM_TEST_EXIT_AFTER_SINK"]
        if not text.isdigit() or int(text) < 1:
            raise StreamSettingsError("STREAM_TEST_EXIT_AFTER_SINK must be a whole number from 1")
        test_exit = int(text)
    cutoff_step_us = round(number(env, "STREAM_CUTOFF_STEP_SECONDS", MINUTE / SECOND, minimum=0) * SECOND)
    if cutoff_step_us < 1:
        raise StreamSettingsError("STREAM_CUTOFF_STEP_SECONDS must be at least one microsecond")
    return StreamSettings(
        bootstrap=required(env, "KAFKA_BOOTSTRAP"),
        registry_url=required(env, "REGISTRY_URL"),
        checkpoint_dir=required(env, "STREAM_CHECKPOINT_DIR"),
        input_topic=env.get("STREAM_INPUT_TOPIC", "") or INPUT_TOPIC,
        output_topic=env.get("STREAM_OUTPUT_TOPIC", "") or FEATURE_READY_TOPIC,
        branch=env.get("STREAM_BRANCH", "") or DEFAULT_BRANCH,
        late_limit_us=round(number(env, "STREAM_LATE_LIMIT_SECONDS", DEFAULT_LATE_LIMIT_SECONDS, minimum=0, allow_zero=True) * SECOND),
        cutoff_step_us=cutoff_step_us,
        trigger_seconds=number(env, "STREAM_TRIGGER_SECONDS", DEFAULT_TRIGGER_SECONDS, minimum=0),
        max_offsets_per_trigger=offset_count(env, "STREAM_MAX_OFFSETS_PER_TRIGGER", DEFAULT_MAX_OFFSETS),
        max_seconds=number(env, "STREAM_MAX_SECONDS", 0.0, minimum=0, allow_zero=True),
        clinical_file=env.get("STREAM_CLINICAL_FILE", ""),
        test_exit_after_sink=test_exit,
    )
