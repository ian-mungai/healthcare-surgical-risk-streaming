"""The copy cost record: the request and transfer cost of the copy into the project's bucket, estimated from the passed check result.

``.venv/bin/python -m scripts.ingestion.copy_cost --result result.json --region <the bucket's region> --since <time> [--until <time>]
--extra-objects <n> --output copy_cost.json``

The result is the file ``scripts.ingestion.cli check`` wrote. It must be a passed result that was not edited. The connection is incremental, so a run
writes only the versions that are new, while the result lists every verified version. The estimate counts the versions whose retrieval time (the time S3
gives the written version) is at or after ``--since`` and, when ``--until`` is given, before it: the start and the end of this copy, as times in the one
form ``YYYY-MM-DDTHH:MM:SS`` then ``Z`` or ``+HH:MM`` or ``-HH:MM``. S3 records a version's time to the whole second, so no fraction is accepted and any
other form is refused. A run that wrote nothing estimates zero payload writes. ``--extra-objects`` is the number of other objects this run landed under the
landing prefix (the metadata records), which the result does not list; the owner reads it from the bucket's listing. The record holds counts, bytes, prices and
money, never a key, a bucket, a version or a label of the result. The same result gives the same bytes. The output is a new file for its owner only.

What the estimate says. The destination writes each payload object to the project's bucket in the host's own region, so the project pays only the write
requests (PUT, COPY, POST and LIST requests share one price). The count depends on the part size of the destination's multipart upload, which the
connection does not set, so the record gives a lowest count (one request for each object) and a highest count (each object as an initiate, parts of 5 MiB,
the smallest part S3 allows, and a complete). The reads of the publisher's public bucket and the data it sends to another region are billed to the
publisher, the bucket's owner, because the bucket is not a Requester Pays bucket (a Requester Pays bucket refuses anonymous requests, which the publisher's
bucket accepts, and its owner pays the data transfer and the requests otherwise). So the transfer billed to the project is zero. The record also states what
the same bytes would cost at the price of inter-region transfer if that were wrong; the bill reading is the check of it: a Cost Explorer line of usage type
``USE1-USW2-AWS-Out-Bytes`` in the project's account would show it.

Prices. Read on Nov 23 2026 from the AWS Price List files (the machine-readable form of the S3 and the data transfer pricing pages):

- ``https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-west-2/index.json``, published 2026-11-21T17:29:19Z: usage type
  ``USW2-Requests-Tier1`` ($0.005 per 1,000 PUT, COPY, POST or LIST requests).
- ``https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/us-east-1/index.json``, published 2026-11-19T10:58:41Z: usage type
  ``USE1-USW2-AWS-Out-Bytes`` ($0.02 per GB to US West (Oregon)).
- ``https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/us-west-2/index.json``, published 2026-11-19T10:58:41Z: usage types
  ``USW2-USE1-AWS-In-Bytes`` and ``USW2-DataTransfer-In-Bytes`` ($0.00 per GB).
- ``https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html`` for who pays and for the refusal of anonymous requests.

The Price List files are per region; this code holds the prices of ``us-west-2`` only and refuses another region. A GB is taken as 2^30 bytes.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from scripts.ingestion.check import CheckResult
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import ClosedError, Code, Notice, render
from scripts.ingestion.output_path import refuse_output_over_input
from scripts.sample.publisher import REGION as PUBLISHER_REGION

COST_FORMAT = "copy_cost_1"
PRICES_READ_ON = "2026-11-23"
PRICE_SOURCES = (
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-west-2/index.json",
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/us-east-1/index.json",
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/us-west-2/index.json",
    "https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html",
)
PUT_TIER_USD_PER_REQUEST: Mapping[str, Decimal] = {"us-west-2": Decimal("0.000005")}
INTER_REGION_OUT_USD_PER_GIB: Mapping[str, Decimal] = {PUBLISHER_REGION: Decimal("0.02")}
SMALLEST_PART_BYTES = 5 * 1024**2
BYTES_PER_GIB = 1024**3
MONEY = Decimal("0.000001")
NOT_COUNTED = (
    "the reads of the publisher's bucket and its data transfer, which the publisher pays",
    "list, head and get requests of the project's bucket, including the check's reads of every landed object",
    "the object storage of the landed copy, billed by the month",
    "the host's compute, its disk and its public address",
)


class CostError(RuntimeError):
    """The estimate cannot be made from the input. The message holds no value of the input."""


def money(value: Decimal) -> str:
    return str(value.quantize(MONEY))


def high_requests(size: int) -> int:
    """The write requests of one object sent as a multipart upload of the smallest parts: an initiate, the parts and a complete."""
    return max(1, math.ceil(size / SMALLEST_PART_BYTES)) + 2


INSTANT_FORM = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])")


def parse_instant(text: str) -> datetime:
    """A time in the one form ``YYYY-MM-DDTHH:MM:SS`` then ``Z`` or ``+HH:MM`` or ``-HH:MM``. Any other text is refused, and the message holds none of it."""
    if INSTANT_FORM.fullmatch(text) is None:
        raise CostError("a time is not in the form YYYY-MM-DDTHH:MM:SS with Z or an offset")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        raise CostError("a time is not in the form YYYY-MM-DDTHH:MM:SS with Z or an offset") from None


def estimate(result: CheckResult, *, region: str, extra_objects: int, since: datetime, until: datetime | None = None) -> dict[str, Any]:
    """The record for a passed, unedited check result, the versions written from ``since`` (and before ``until``) and the other objects this run landed."""
    if not result.passed:
        raise ClosedError(Notice(Code.NOT_PASSED))
    if result.claimed_sha256 != result.result_sha256:
        raise ClosedError(Notice(Code.EDITED))
    if region not in PUT_TIER_USD_PER_REQUEST:
        raise CostError("the code holds no prices for this region")
    if extra_objects < 0:
        raise CostError("the number of extra objects is below zero")
    if until is not None and until <= since:
        raise CostError("the end of the window is not after its start")
    if any(file.size < 0 for file in result.files):
        raise CostError("a size in the result is below zero")
    sizes = [file.size for file in result.files if since <= parse_instant(file.retrieved_utc) and (until is None or parse_instant(file.retrieved_utc) < until)]
    price = PUT_TIER_USD_PER_REQUEST[region]
    low = len(sizes) + extra_objects
    high = sum(high_requests(size) for size in sizes) + 3 * extra_objects
    request_cost = {"low": money(low * price), "high": money(high * price)}
    transfer_if_billed = Decimal(sum(sizes)) / BYTES_PER_GIB * INTER_REGION_OUT_USD_PER_GIB[PUBLISHER_REGION]
    return {
        "format": COST_FORMAT,
        "kind": "estimate",
        "check_result_sha256": result.result_sha256,
        "region": region,
        "source_region": PUBLISHER_REGION,
        "prices": {
            "read_on": PRICES_READ_ON,
            "sources": list(PRICE_SOURCES),
            "put_tier_usd_per_request": str(price.normalize()),
            "inter_region_out_usd_per_gib": str(INTER_REGION_OUT_USD_PER_GIB[PUBLISHER_REGION].normalize()),
        },
        "window": {"since": since.isoformat(), "until": until.isoformat() if until else None, "time_precision_seconds": 1},
        "copy": {"verified_objects": len(result.files), "payload_objects": len(sizes), "payload_bytes": sum(sizes), "extra_objects": extra_objects},
        "write_requests": {"low": low, "high": high},
        "request_cost_usd": request_cost,
        "transfer_cost_usd": {
            "billed_to_project": money(Decimal(0)),
            "basis": (
                "the publisher's bucket is not Requester Pays, so its owner pays the data transfer out; "
                "data into the host and into a bucket in its region is free"
            ),
            "if_requester_pays": money(transfer_if_billed),
        },
        "total_usd": request_cost,
        "not_counted": list(NOT_COUNTED),
    }


def write_record(path: Path, document: Mapping[str, Any]) -> None:
    """Create the output for its owner only. An existing path, also a link, is refused and left as it was."""
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise CostError("the output path exists") from None
    except OSError:
        raise CostError("the output path cannot be created") from None
    with os.fdopen(descriptor, "w") as handle:
        handle.write(json.dumps(document, indent=1) + "\n")


def whole_number(text: str) -> int:
    if not (text.isascii() and text.isdigit()):
        raise argparse.ArgumentTypeError("a count is a whole number")
    return int(text)


def instant_argument(text: str) -> datetime:
    try:
        return parse_instant(text)
    except CostError:
        raise argparse.ArgumentTypeError("a time is an ISO 8601 time with a zone") from None


def main(argv: Sequence[str] | None = None) -> int:
    command = QuietParser(prog="python -m scripts.ingestion.copy_cost", description=(__doc__ or "").split("\n\n")[0])
    command.add_argument("--result", type=Path, required=True, help="the passed check result of the copy")
    command.add_argument("--region", required=True, help="the region of the project's bucket and of the host")
    command.add_argument(
        "--since",
        type=instant_argument,
        required=True,
        help="the start of the copy as YYYY-MM-DDTHH:MM:SS then Z or +HH:MM or -HH:MM, with no fraction: S3 records a version's time to the second",
    )
    command.add_argument(
        "--until", type=instant_argument, help="the end of the copy, in the form of --since with no fraction; the run's last write when absent"
    )
    command.add_argument("--extra-objects", type=whole_number, default=0, help="the other objects the copy landed under the landing prefix")
    command.add_argument("--output", type=Path, required=True, help="a new file for the record")
    arguments = command.parse_args(argv)
    try:
        refuse_output_over_input([arguments.output], [arguments.result])
        try:
            text = arguments.result.read_text()
        except OSError, UnicodeDecodeError:
            raise CostError("the check result cannot be read") from None
        document = estimate(
            CheckResult.from_json(text), region=arguments.region, extra_objects=arguments.extra_objects, since=arguments.since, until=arguments.until
        )
        write_record(arguments.output, document)
    except ClosedError as error:
        sys.stderr.write("".join(f"{render(notice)}\n" for notice in error.notices))
        return 1
    except CostError as error:
        sys.stderr.write(f"copy cost: {error}\n")
        return 1
    sys.stdout.write(
        f"copy cost: {document['copy']['payload_objects']} payload objects written of {document['copy']['verified_objects']} verified, "
        f"{document['copy']['payload_bytes']} bytes; "
        f"write requests {document['write_requests']['low']} to {document['write_requests']['high']}; "
        f"request cost {document['request_cost_usd']['low']} to {document['request_cost_usd']['high']} USD; "
        f"transfer billed to the project {document['transfer_cost_usd']['billed_to_project']} USD\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
