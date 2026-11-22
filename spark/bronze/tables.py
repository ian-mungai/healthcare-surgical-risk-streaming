"""The Bronze tables: their names, columns and stable keys, as data. Nothing here imports Spark or the publisher's library.

Bronze holds what the case files publish and changes no value. The namespace ``bronze`` of the catalog ``lake`` holds four tables:

- ``numeric_observations``: one row per numeric value. Key: case, track, case time in microseconds and ``source_occurrence``, the position of the reading among
  the readings of its track that share its case time, in the order of the file.
- ``waveform_seconds``: one row per case, track and second, holding that second's samples in the order of the file. Key: case, track and second. The samples
  are doubles whatever the track's own sample type is (the types of the format are all held exactly by a double), and the track's type is in ``tracks``.
- ``tracks``: one row per track of each case with its device, name, unit and the publisher's track metadata. Key: case and track.
- ``string_events``: one row per published string event, with its original time, text and stable source ID. Key as for numeric readings.

Every row carries its lineage: the object version of the case file it was decoded from and that file's SHA-256 from the manifest of the passed check.
A track's name is the library's name for it: the device's name, a slash and the track's own name (just the track's own name when it has no device).
"""

from __future__ import annotations

from dataclasses import dataclass

CATALOG = "lake"
NAMESPACE = "bronze"
LINEAGE_COLUMNS = (("source_version_id", "string"), ("source_sha256", "string"))


@dataclass(frozen=True, slots=True)
class TableSpec:
    name: str
    columns: tuple[tuple[str, str], ...]
    key: tuple[str, ...]

    @property
    def full_name(self) -> str:
        return f"{CATALOG}.{NAMESPACE}.{self.name}"

    @property
    def ddl(self) -> str:
        """The columns as a Spark DDL schema text, in the order of the rows."""
        return ", ".join(f"{name} {kind}" for name, kind in self.columns)


NUMERIC = TableSpec(
    "numeric_observations",
    (
        ("case_id", "bigint"),
        ("track", "string"),
        ("case_time_us", "bigint"),
        ("source_occurrence", "int"),
        ("source_time", "double"),
        ("value", "double"),
        *LINEAGE_COLUMNS,
    ),
    ("case_id", "track", "case_time_us", "source_occurrence"),
)
WAVEFORM = TableSpec(
    "waveform_seconds",
    (
        ("case_id", "bigint"),
        ("track", "string"),
        ("case_second", "bigint"),
        ("start_offset_us", "bigint"),
        ("samples", "array<double>"),
        *LINEAGE_COLUMNS,
    ),
    ("case_id", "track", "case_second"),
)
TRACKS = TableSpec(
    "tracks",
    (
        ("case_id", "bigint"),
        ("track", "string"),
        ("device", "string"),
        ("track_name", "string"),
        ("device_type", "string"),
        ("device_port", "string"),
        ("kind", "string"),
        ("track_type", "int"),
        ("sample_format", "int"),
        ("unit", "string"),
        ("sample_rate", "double"),
        ("gain", "double"),
        ("value_offset", "double"),
        ("min_display", "double"),
        ("max_display", "double"),
        ("color", "bigint"),
        ("monitor_type", "int"),
        *LINEAGE_COLUMNS,
    ),
    ("case_id", "track"),
)
STRINGS = TableSpec(
    "string_events",
    (
        ("case_id", "bigint"),
        ("track", "string"),
        ("case_time_us", "bigint"),
        ("source_occurrence", "int"),
        ("source_time", "double"),
        ("text", "string"),
        ("source_id", "string"),
        *LINEAGE_COLUMNS,
    ),
    ("case_id", "track", "case_time_us", "source_occurrence"),
)
TABLES = (NUMERIC, WAVEFORM, TRACKS, STRINGS)
KIND_TABLE = {"numeric": NUMERIC, "waveform": WAVEFORM, "tracks": TRACKS, "string": STRINGS}
