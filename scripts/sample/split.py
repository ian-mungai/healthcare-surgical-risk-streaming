"""The patient assignment rule: one split for all models, frozen.

A patient's part depends only on the publisher's patient number and the frozen seed ``SEED``, which no caller can change, so every tool computes the same
answer in batch and on the stream, with no table lookup. The bucket is the first 15 hex digits (60 bits, so a signed 64-bit integer holds them) of the
SHA-256 of the text ``<seed>:<patient number>``, taken modulo 100. Buckets 0 to 69 are the training part, 70 to 79 the validation part and 80 to 99 the
test part. Training and validation together are the development data. The approved analysis plan sets the shares at 70, 10 and 20 percent by patient.
"""

from __future__ import annotations

import hashlib
from typing import Literal

Part = Literal["train", "validation", "test"]

SEED = 1
BUCKETS = 100
TRAIN_BUCKETS = 70
VALIDATION_BUCKETS = 10
HEX_DIGITS = 15


def patient_bucket(patient: int) -> int:
    """The bucket, from 0 to 99, of the publisher's patient number."""
    if patient < 1:
        raise ValueError("not a patient number: a patient number is 1 or more")
    digest = hashlib.sha256(f"{SEED}:{patient}".encode()).hexdigest()
    return int(digest[:HEX_DIGITS], 16) % BUCKETS


def patient_part(patient: int) -> Part:
    """The part that holds all cases of the patient."""
    bucket = patient_bucket(patient)
    if bucket < TRAIN_BUCKETS:
        return "train"
    return "validation" if bucket < TRAIN_BUCKETS + VALIDATION_BUCKETS else "test"


def is_development(patient: int) -> bool:
    """True for the training and validation parts, which model fitting and tuning may use."""
    return patient_part(patient) != "test"
