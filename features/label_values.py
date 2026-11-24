"""The three values of an outcome label and its eligibility: a label is positive or negative when the observations establish it, and unknown when they do not.

Unknown labels are excluded from the primary analyses and counted (analysis plan, "Cohorts"); a label row is eligible exactly when its label is not unknown.
"""

from __future__ import annotations

POSITIVE = "positive"
NEGATIVE = "negative"
UNKNOWN = "unknown"
LABEL_VALUES = (POSITIVE, NEGATIVE, UNKNOWN)
