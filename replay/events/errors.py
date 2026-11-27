"""The error that every refusal of the event model raises."""

from __future__ import annotations


class EventModelError(ValueError):
    """An identity, a recording, a plan or a schedule that the event model cannot accept. The message names the part and never a reading."""
