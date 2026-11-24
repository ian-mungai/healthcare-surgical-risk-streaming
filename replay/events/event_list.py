"""The version of the list of events that a case sends, and the code of the refusal for a saved file that counts another list.

Bump ``EVENT_LIST_VERSION`` when the events of a case change in content, order or count (``events_3``: exact sampling changed the waveform seconds). A saved
position counts the old list, and so does a frozen schedule, whose first due instant is the earliest due time of the list that froze it. Both name the
version they were written for and are refused, with ``EVENT_LIST_REFUSAL_CODE`` in the message, when it is missing or another one.
"""

from __future__ import annotations

EVENT_LIST_VERSION = "events_3"
EVENT_LIST_REFUSAL_CODE = "replay_event_list_refused"
