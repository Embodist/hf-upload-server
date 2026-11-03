#!/usr/bin/env bash
set -euo pipefail

BASE="${HF_UPLOAD_BASE:-http://127.0.0.1:8000}"
KEY="${HF_SERVER_API_KEY:-}"
AUTH=()
if [[ -n "$KEY" ]]; then
  AUTH=(-H "X-API-Key: $KEY")
fi

cmd="${1:-}"
shift || true

case "$cmd" in
  file)
    path="${1:?usage: curl.sh file <path> [dir]}"
    dir="${2:-uploads}"
    curl -sS -X POST "$BASE/upload/file" ${AUTH[@]+"${AUTH[@]}"} \
      -F "files=@$path" -F "dir=$dir" -F "wait=true" | python3 -m json.tool
    ;;
  url)
    url="${1:?usage: curl.sh url <url> [dir]}"
    dir="${2:-uploads}"
    curl -sS -X POST "$BASE/upload/url" ${AUTH[@]+"${AUTH[@]}"} \
      -H "Content-Type: application/json" \
      -d "{\"urls\":[\"$url\"],\"dir\":\"$dir\",\"wait\":true}" | python3 -m json.tool
    ;;
  stream)
    path="${1:?usage: curl.sh stream <path> [dir]}"
    dir="${2:-uploads}"
    name="$(basename "$path")"
    curl -sS -X POST "$BASE/upload/stream?filename=$name&dir=$dir&wait=true" \
      ${AUTH[@]+"${AUTH[@]}"} --data-binary "@$path" | python3 -m json.tool
    ;;
  queue)
    curl -sS "$BASE/queue" ${AUTH[@]+"${AUTH[@]}"} | python3 -m json.tool
    ;;
  metrics)
    curl -sS "$BASE/metrics" ${AUTH[@]+"${AUTH[@]}"}
    ;;
  job)
    curl -sS "$BASE/jobs/${1:?usage: curl.sh job <job_id>}" ${AUTH[@]+"${AUTH[@]}"} | python3 -m json.tool
    ;;
  *)
    cat <<'USAGE'
usage:
  HF_SERVER_API_KEY=... ./curl.sh file   <path> [dir]
  HF_SERVER_API_KEY=... ./curl.sh url    <url>  [dir]
  HF_SERVER_API_KEY=... ./curl.sh stream <path> [dir]
  HF_SERVER_API_KEY=... ./curl.sh queue
  HF_SERVER_API_KEY=... ./curl.sh metrics
  HF_SERVER_API_KEY=... ./curl.sh job <job_id>
USAGE
    ;;
esac
