"""Renew each workload's short-lived AWS credentials on the host.

A systemd timer runs this as root. For each workload in ``/etc/workload_access/workloads.json`` it keeps
``/run/workload_access/<name>/credentials.json`` fresh: it assumes the workload's own role with the host role's
credentials and writes the result in the format of the AWS ``credential_process`` setting, readable by the workload's
group only. It renews a file that is missing, malformed or within thirty minutes of expiry, so a workload always holds
credentials that are valid and never the host's. It uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import os
import re
import sys
import urllib.parse
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

CONFIGURATION = Path("/etc/workload_access/workloads.json")
TARGET = Path("/run/workload_access")
METADATA_HOST = "169.254.169.254"
RENEW_BEFORE = timedelta(minutes=30)
SESSION_SECONDS = 3600
REQUEST_TIMEOUT_SECONDS = 20
EXPIRATION_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
CREDENTIAL_FIELDS = ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration")
# The form of a credential value that goes into a header or a credential file: the characters of a bearer token (RFC 6750, b64token) and nothing else. A
# control character such as a line break makes the HTTP library's header error carry the value's text. The expiration is a time of its own form.
VALUE_FORM = re.compile(r"[A-Za-z0-9\-._~+/]+=*")
EXPIRATION_FORM = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")


class CredentialError(RuntimeError):
    """Instance metadata or STS did not return usable credentials."""


def needs_renewal(document: dict[str, Any] | None, now: datetime) -> bool:
    """Whether the stored credentials are missing, malformed or within the renewal window."""
    if not document or any(not document.get(field) for field in CREDENTIAL_FIELDS):
        return True
    try:
        expiration = datetime.strptime(str(document["Expiration"]), EXPIRATION_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return True
    return expiration - now < RENEW_BEFORE


def signing_key(secret: str, date: str, region: str, service: str) -> bytes:
    key = ("AWS4" + secret).encode()
    for part in (date, region, service, "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    return key


def header_value(credentials: dict[str, str], field: str) -> str:
    """The credential ``field`` of the host role when it is text of the closed header-safe form, or ``CredentialError`` that never holds the value."""
    value = credentials.get(field)
    if not isinstance(value, str) or VALUE_FORM.fullmatch(value) is None:
        raise CredentialError(f"the host role's {field} is not a valid credential value")
    return value


def signed_headers(credentials: dict[str, str], region: str, body: str, now: datetime) -> dict[str, str]:
    """The headers of a Signature Version 4 POST to the regional STS endpoint."""
    token = header_value(credentials, "Token")
    access_key = header_value(credentials, "AccessKeyId")
    host = f"sts.{region}.amazonaws.com"
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    headers = {
        "content-type": "application/x-www-form-urlencoded; charset=utf-8",
        "host": host,
        "x-amz-date": stamp,
        "x-amz-security-token": token,
    }
    names = ";".join(headers)
    canonical = "\n".join(["POST", "/", "", *[f"{name}:{value}" for name, value in headers.items()], "", names, hashlib.sha256(body.encode()).hexdigest()])
    scope = f"{stamp[:8]}/{region}/sts/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    signature = hmac.new(signing_key(credentials["SecretAccessKey"], stamp[:8], region, "sts"), to_sign.encode(), hashlib.sha256).hexdigest()
    return {
        "Content-Type": headers["content-type"],
        "Host": host,
        "X-Amz-Date": stamp,
        "X-Amz-Security-Token": token,
        "Authorization": f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={names}, Signature={signature}",
    }


def credential_document(body: str) -> dict[str, Any]:
    """The ``credential_process`` document from an STS AssumeRole response."""
    found = {field: re.search(rf"<{field}>([^<]+)</{field}>", body) for field in CREDENTIAL_FIELDS}
    if any(match is None for match in found.values()):
        raise CredentialError("the STS response holds no credentials")
    document: dict[str, Any] = {"Version": 1}
    for field, match in found.items():
        form = EXPIRATION_FORM if field == "Expiration" else VALUE_FORM
        if match is None or form.fullmatch(match.group(1)) is None:
            raise CredentialError(f"the STS response's {field} is not a valid credential value")
        document[field] = match.group(1)
    return document


def metadata(method: str, path: str, headers: dict[str, str]) -> str:
    connection = http.client.HTTPConnection(METADATA_HOST, timeout=REQUEST_TIMEOUT_SECONDS)
    try:
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        text = response.read().decode()
    finally:
        connection.close()
    if response.status != http.client.OK:
        raise CredentialError(f"instance metadata answered {response.status} for {path}")
    return text


def host_credentials() -> dict[str, str]:
    session = metadata("PUT", "/latest/api/token", {"X-aws-ec2-metadata-token-ttl-seconds": "60"})
    if VALUE_FORM.fullmatch(session) is None:
        raise CredentialError("instance metadata answered a session token that is not a valid token")
    token = {"X-aws-ec2-metadata-token": session}
    role = metadata("GET", "/latest/meta-data/iam/security-credentials/", token).strip()
    credentials: dict[str, str] = json.loads(metadata("GET", f"/latest/meta-data/iam/security-credentials/{role}", token))
    return credentials


def assume_role(credentials: dict[str, str], region: str, role_arn: str, session_name: str, now: datetime) -> dict[str, Any]:
    form = {"Action": "AssumeRole", "Version": "2011-06-15", "RoleArn": role_arn, "RoleSessionName": session_name, "DurationSeconds": str(SESSION_SECONDS)}
    body = urllib.parse.urlencode(form)
    connection = http.client.HTTPSConnection(f"sts.{region}.amazonaws.com", timeout=REQUEST_TIMEOUT_SECONDS)
    try:
        connection.request("POST", "/", body=body, headers=signed_headers(credentials, region, body, now))
        response = connection.getresponse()
        text = response.read().decode()
    finally:
        connection.close()
    if response.status != http.client.OK:
        code = re.search(r"<Code>([^<]+)</Code>", text)
        raise CredentialError(f"STS refused the role for {session_name}: {code.group(1) if code else response.status}")
    return credential_document(text)


def write_credentials(name: str, group_id: int, document: dict[str, Any]) -> None:
    folder = TARGET / name
    folder.mkdir(mode=0o750, parents=True, exist_ok=True)
    os.chown(folder, 0, group_id)
    partial = folder / "credentials.json.partial"
    partial.write_text(json.dumps(document))
    os.chown(partial, 0, group_id)
    os.chmod(partial, 0o640)
    partial.replace(folder / "credentials.json")


def stored(name: str) -> dict[str, Any] | None:
    path = TARGET / name / "credentials.json"
    try:
        document: dict[str, Any] = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return document


def main() -> int:
    """Renew every workload that needs it and return 1 when one could not be renewed."""
    os.umask(0o027)
    configuration = json.loads(CONFIGURATION.read_text())
    now = datetime.now(UTC)
    due = [workload for workload in configuration["workloads"] if needs_renewal(stored(workload["name"]), now)]
    if not due:
        sys.stdout.write("no workload needs new credentials\n")
        return 0
    failed = 0
    try:
        credentials = host_credentials()
    except (CredentialError, OSError) as error:
        sys.stderr.write(f"cannot read the host role's credentials: {error}\n")
        return 1
    for workload in due:
        try:
            write_credentials(
                workload["name"], int(workload["gid"]), assume_role(credentials, configuration["region"], workload["role_arn"], workload["name"], now)
            )
            sys.stdout.write(f"renewed: {workload['name']}\n")
        except (CredentialError, OSError) as error:
            failed += 1
            sys.stderr.write(f"{workload['name']}: {error}\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
