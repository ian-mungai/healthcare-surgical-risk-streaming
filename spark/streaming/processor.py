"""The keyed operator: Spark's ``transformWithState`` over the key run, room and case occurrence, with one JSON state value for each key.

The processor class is built inside a function because it extends a PySpark class, and PySpark is in the job image only. It holds no logic: it
reads the state, calls ``spark.streaming.occurrence.process`` and writes the state back. Spark 4.1.4's Python ``transformWithState`` (the row
API, new in 4.1.0) needs RocksDB as the state store provider and the packages ``pyarrow`` and ``protobuf``. It is used with the time mode
``None``: in the event-time mode Spark drops the rows behind the watermark before the processor sees them, so no decision could be recorded for
them.
"""

from __future__ import annotations

import importlib
from typing import Any

from spark.streaming.clinical import load_clinical
from spark.streaming.model import Context, Policy
from spark.streaming.occurrence import process
from spark.streaming.rows import STATE_DDL, event_from_row, record_row
from spark.streaming.state import OccurrenceState
from spark.streaming.version import feature_version

STATE_NAME = "occurrence"
STATE_PROVIDER = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"


def make_processor(late_limit_us: int, cutoff_step_us: int, clinical_file: str) -> Any:
    """A stateful processor for the job's settings, to hand to ``transformWithState``."""
    row_class = importlib.import_module("pyspark.sql").Row
    base = importlib.import_module("pyspark.sql.streaming").StatefulProcessor

    class OccurrenceProcessor(base):  # type: ignore[misc,valid-type]
        def init(self, handle: Any) -> None:
            self.state = handle.getValueState(STATE_NAME, STATE_DDL)
            self.context = Context(Policy(late_limit_us, cutoff_step_us), feature_version(), load_clinical(clinical_file))

        def handleInputRows(self, key: Any, rows: Any, timerValues: Any) -> Any:
            events = [event_from_row(row) for row in rows]
            if not events:
                return iter([])
            held = self.state.get() if self.state.exists() else None
            state, records = process(OccurrenceState.from_json(held[0]) if held else None, events, self.context)
            self.state.update((state.to_json(),))
            return iter([row_class(**record_row(record)) for record in records])

        def close(self) -> None:
            return None

    return OccurrenceProcessor()
