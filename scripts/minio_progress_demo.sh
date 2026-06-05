#!/usr/bin/env bash
#
# Manual demo for the fetch/pull/push progress bars against a local minio.
#
# The progress bar only renders on a TTY, so run this directly in your terminal
# (not piped) to watch the byte/rate postfix climb live.
#
# Usage:
#   scripts/minio_progress_demo.sh all       # setup + push + fetch (default)
#   scripts/minio_progress_demo.sh setup     # start minio, make project + big file, track
#   scripts/minio_progress_demo.sh push      # watch the upload bar
#   scripts/minio_progress_demo.sh fetch     # clear cache, watch the download bar
#   scripts/minio_progress_demo.sh cleanup   # stop minio, remove temp files
#
# Env overrides:
#   SIZE_MB=4096   # size of the demo blob in MiB (default 2048)
#   PORT=9000      # minio port (default 9000)
#   FILES=1        # number of blobs to track (set >1 to watch the file-count bar move)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK=/tmp/pivot-minio-demo
PROJECT="$WORK/project"
SIZE_MB="${SIZE_MB:-2048}"
PORT="${PORT:-9000}"
FILES="${FILES:-1}"
BUCKET=pivot-demo
CONTAINER=pivot-minio

# Prefer the repo venv; fall back to `uv run`.
if [[ -x "$REPO/.venv/bin/pivot" ]]; then
  PIVOT="$REPO/.venv/bin/pivot"
  PY="$REPO/.venv/bin/python"
else
  PIVOT="uv run --project $REPO pivot"
  PY="uv run --project $REPO python"
fi

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL="http://localhost:$PORT"
export AWS_ENDPOINT_URL_S3="http://localhost:$PORT"
export AWS_CONFIG_FILE="$WORK/aws-config"

_make_big_file() {
  local path="$1"
  if command -v mkfile >/dev/null 2>&1; then        # macOS
    mkfile "${SIZE_MB}m" "$path"
  else                                               # linux fallback
    head -c "$((SIZE_MB * 1024 * 1024))" /dev/zero > "$path"
  fi
}

setup() {
  echo ">> starting minio container '$CONTAINER' on port $PORT"
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$CONTAINER" -p "$PORT:9000" \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data >/dev/null

  echo ">> writing AWS config (path-style addressing) to $AWS_CONFIG_FILE"
  mkdir -p "$WORK"
  printf '[default]\ns3 =\n    addressing_style = path\n' > "$AWS_CONFIG_FILE"

  echo ">> waiting for minio + creating bucket '$BUCKET'"
  for _ in $(seq 1 30); do
    if $PY -c "
import boto3, botocore
s3 = boto3.client('s3')
try:
    s3.create_bucket(Bucket='$BUCKET')
except botocore.exceptions.ClientError as e:
    if 'BucketAlreadyOwnedByYou' not in str(e) and 'BucketAlreadyExists' not in str(e):
        raise
print('bucket ready')
" 2>/dev/null; then
      break
    fi
    sleep 1
  done

  echo ">> creating project at $PROJECT with ${FILES}x ${SIZE_MB}MiB file(s)"
  rm -rf "$PROJECT"
  mkdir -p "$PROJECT"
  cd "$PROJECT"
  git init -q
  $PIVOT init >/dev/null
  $PIVOT config set remotes.minio "s3://$BUCKET/cache" >/dev/null
  for i in $(seq 1 "$FILES"); do
    _make_big_file "big$i.bin"
  done
  $PIVOT track big*.bin
  echo ">> setup complete"
}

push() {
  cd "$PROJECT"
  echo ">> pivot push (watch the upload bar)"
  $PIVOT push
}

fetch() {
  cd "$PROJECT"
  echo ">> clearing local cache, then pivot fetch (watch the download bar)"
  rm -rf .pivot/cache
  $PIVOT fetch
}

cleanup() {
  echo ">> removing minio container and temp files"
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}

case "${1:-all}" in
  setup)   setup ;;
  push)    push ;;
  fetch)   fetch ;;
  all)     setup; push; fetch ;;
  cleanup) cleanup ;;
  *) echo "Usage: $0 [all|setup|push|fetch|cleanup]" >&2; exit 2 ;;
esac
