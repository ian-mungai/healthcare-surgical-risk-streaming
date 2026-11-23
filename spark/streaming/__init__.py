"""The streaming job: Kafka events in, feature rows in the lake and feature-ready events out.

Everything except ``processor.py``, ``query.py`` and ``sinks.py``'s Spark calls is plain Python with no Spark and no Kafka in it, so the fast tests prove it.
The feature values come from ``features.row.build_row``, the same entry point the batch path calls.
"""
