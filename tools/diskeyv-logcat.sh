#!/usr/bin/env bash
set -euo pipefail

log_file=${1:-${DISKEYV_LOG_FILE:-/tmp/diskeyv.log}}
level_filter=${2:-}
tag_filter=${3:-}
history_lines=${DISKEYV_LOGCAT_LINES:-100}

if [[ -n "$level_filter" && ! "$level_filter" =~ ^(D|I|W|E)$ ]]; then
    echo "Level must be D, I, W, E, or empty" >&2
    exit 2
fi

mkdir -p "$(dirname "$log_file")"
touch "$log_file"
echo "Following $log_file (level=${level_filter:-ALL}, tag=${tag_filter:-ALL})" >&2

tail -n "$history_lines" -F "$log_file" |
while IFS= read -r line; do
    if [[ -n "$level_filter" && "$line" != *" $level_filter/"* ]]; then
        continue
    fi
    if [[ -n "$tag_filter" && "$line" != *"/$tag_filter("* ]]; then
        continue
    fi
    printf '%s\n' "$line"
done
