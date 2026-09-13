#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DISKEYV_LEADER_WORKERS=1

engine=${DISKEYV_CONTAINER_ENGINE:-auto}
docker_command=$(command -v docker 2>/dev/null || true)
if [[ "$engine" == "podman" ]] ||
   { [[ "$engine" == "auto" ]] && command -v podman >/dev/null 2>&1 &&
     { [[ -z "$docker_command" ]] ||
       grep -a -q "exec /usr/bin/podman" "$docker_command" 2>/dev/null; }; }; then
    exec "$repo_dir/tests/podman_smoke.sh"
fi

if [[ "$engine" != "auto" && "$engine" != "docker" ]]; then
    echo "DISKEYV_CONTAINER_ENGINE must be auto, docker, or podman" >&2
    exit 2
fi
if [[ -z "$docker_command" ]]; then
    echo "Neither Docker nor Podman is available" >&2
    exit 2
fi

compose=(docker compose -f "$repo_dir/compose.yaml")

cleanup() {
    "${compose[@]}" down --volumes --remove-orphans
}
trap cleanup EXIT

"${compose[@]}" up --build --detach --wait

"${compose[@]}" run --rm --no-deps client put 5000 docker-smoke 7,8,9
leader_read=$("${compose[@]}" run --rm --no-deps client get 5000 docker-smoke)
follower1_read=$("${compose[@]}" run --rm --no-deps \
    -e DISKEYV_HOST=follower1 client get 5000 docker-smoke)
follower2_read=$("${compose[@]}" run --rm --no-deps \
    -e DISKEYV_HOST=follower2 client get 5000 docker-smoke)

grep -q "Value: 7,8,9" <<<"$leader_read"
grep -q "Value: 7,8,9" <<<"$follower1_read"
grep -q "Value: 7,8,9" <<<"$follower2_read"

"${compose[@]}" stop follower2
"${compose[@]}" run --rm --no-deps client put 5000 one-follower-down 1

"${compose[@]}" start follower2
restarted_healthy=false
for _ in {1..40}; do
    if "${compose[@]}" exec -T follower2 \
        /usr/local/bin/diskeyv-client health 5000 >/dev/null 2>&1; then
        restarted_healthy=true
        break
    fi
    sleep 0.5
done
if [[ "$restarted_healthy" != true ]]; then
    echo "Restarted follower2 did not become healthy" >&2
    "${compose[@]}" logs follower2 >&2 || true
    exit 1
fi
"${compose[@]}" run --rm --no-deps client put 5000 repair-trigger 4
repaired_initial=$("${compose[@]}" run --rm --no-deps \
    -e DISKEYV_HOST=follower2 client get 5000 docker-smoke)
repaired_missed=$("${compose[@]}" run --rm --no-deps \
    -e DISKEYV_HOST=follower2 client get 5000 one-follower-down)
grep -q "Value: 7,8,9" <<<"$repaired_initial"
grep -q "Value: 1" <<<"$repaired_missed"

"${compose[@]}" stop follower1
"${compose[@]}" run --rm --no-deps client put 5000 repaired-follower-quorum 5

"${compose[@]}" stop follower2
if "${compose[@]}" run --rm --no-deps client put 5000 no-quorum 2; then
    echo "FAIL: PUT succeeded without a replication quorum" >&2
    exit 1
fi

echo "Docker smoke test passed"
