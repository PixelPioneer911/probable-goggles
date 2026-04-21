#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
Usage: ./run_bot_workflow.sh <fetch|download> [--retry-failed]

Commands:
  fetch      Run the bot fetch script and write a new fetched_links JSON file.
  download   Download price lists for every bot in the latest fetch file.

Options:
  --retry-failed   With download, retry only the failed links from the most recent run.
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

command_name="$1"
retry_flag="${2:-}"

case "$command_name" in
  fetch)
    if [[ -n "$retry_flag" ]]; then
      echo "fetch does not accept extra arguments" >&2
      usage
      exit 1
    fi
    exec python3 "$SCRIPT_DIR/scripts/fetch_bots.py"
    ;;
  download)
    if [[ -n "$retry_flag" && "$retry_flag" != "--retry-failed" ]]; then
      echo "Unknown download option: $retry_flag" >&2
      usage
      exit 1
    fi
    if [[ "$retry_flag" == "--retry-failed" ]]; then
      exec python3 "$SCRIPT_DIR/scripts/run_latest_fetched_bot.py" --retry-failed
    fi
    exec python3 "$SCRIPT_DIR/scripts/run_latest_fetched_bot.py"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: $command_name" >&2
    usage
    exit 1
    ;;
esac
