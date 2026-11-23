#!/bin/sh
# Runs as the numeric user that owns both secret files. It gives the unlock key to OpenBao in its process environment only, so it is not in the container's configuration. The bootstrap secret ID stays in its file: the first start's self-initialization reads that file once (openbao.hcl) and no process environment holds it.
set -eu
OPENBAO_UNLOCK_KEY="$(sed -n 's/^OPENBAO_UNLOCK_KEY=//p' /run/secrets/openbao_unlock_key_file | head -n 1)"
if [ -z "$OPENBAO_UNLOCK_KEY" ]; then
  echo "the unlock key file holds no OPENBAO_UNLOCK_KEY line" >&2
  exit 1
fi
if [ ! -s /run/secrets/openbao_bootstrap_secret_id ]; then
  echo "the bootstrap secret ID file is missing or empty" >&2
  exit 1
fi
export OPENBAO_UNLOCK_KEY
exec bao server -config=/openbao/config
