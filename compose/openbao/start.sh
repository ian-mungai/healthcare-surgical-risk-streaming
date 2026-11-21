#!/bin/sh
# Runs as the numeric user that owns both secret files. It gives their values to OpenBao in its process environment only, so neither is in the container's configuration.
set -eu
OPENBAO_UNLOCK_KEY="$(sed -n 's/^OPENBAO_UNLOCK_KEY=//;T;p;q' /run/secrets/openbao_unlock_key_file)"
OPENBAO_BOOTSTRAP_SECRET_ID="$(cat /run/secrets/openbao_bootstrap_secret_id)"
if [ -z "$OPENBAO_UNLOCK_KEY" ] || [ -z "$OPENBAO_BOOTSTRAP_SECRET_ID" ]; then
  echo "the unlock key file holds no OPENBAO_UNLOCK_KEY line or the bootstrap secret ID file is empty" >&2
  exit 1
fi
export OPENBAO_UNLOCK_KEY OPENBAO_BOOTSTRAP_SECRET_ID
exec bao server -config=/openbao/config
