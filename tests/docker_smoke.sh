#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DISKEYV_LEADER_WORKERS=1
engine=${DISKEYV_CONTAINER_ENGINE:-auto}
docker_command=$(command -v docker 2>/dev/null || true)
if [[ "$engine" == podman ]] ||
   { [[ "$engine" == auto ]] && command -v podman >/dev/null 2>&1 &&
     { [[ -z "$docker_command" ]] ||
       grep -a -q "exec /usr/bin/podman" "$docker_command" 2>/dev/null; }; }; then
    exec "$repo_dir/tests/podman_smoke.sh"
fi
[[ "$engine" == auto || "$engine" == docker ]] || exit 2
[[ -n "$docker_command" ]] || { echo "Docker is unavailable" >&2; exit 2; }

compose=(docker compose -f "$repo_dir/compose.yaml")
replicas=(leader follower1 follower2 follower3 follower4)
cleanup() { "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

run_client() {
    local replica=$1
    shift
    "${compose[@]}" exec -T -e DISKEYV_HOST=127.0.0.1 "$replica" \
        /usr/local/bin/diskeyv-client "$@"
}

probe_leader() {
    local attempt=$1 winners=() index=0
    for replica in "${replicas[@]}"; do
        if run_client "$replica" put 5000 "docker-probe-${attempt}-${index}" \
            "$index" >/dev/null 2>&1; then winners+=("$replica"); fi
        index=$((index + 1))
    done
    ((${#winners[@]} == 1)) || return 1
    echo "${winners[0]}"
}

wait_for_leader() {
    local candidate confirmation
    for attempt in {1..80}; do
        candidate=$(probe_leader "$attempt" || true)
        if [[ -n "$candidate" ]]; then
            sleep 0.2
            confirmation=$(probe_leader "confirm-$attempt" || true)
            [[ "$confirmation" == "$candidate" ]] && { echo "$candidate"; return; }
        fi
        sleep 0.1
    done
    "${compose[@]}" logs >&2 || true
    return 1
}

wait_for_value() {
    local replica=$1 key=$2 expected=$3 output
    for _ in {1..50}; do
        output=$(run_client "$replica" get 5000 "$key" 2>/dev/null || true)
        grep -q "Value: $expected" <<<"$output" && return
        sleep 0.1
    done
    return 1
}

"${compose[@]}" up --build --detach --wait
elected=$(wait_for_leader)
followers=(); for replica in "${replicas[@]}"; do [[ "$replica" == "$elected" ]] || followers+=("$replica"); done

run_client "$elected" put 5000 docker-smoke 7,8,9 >/dev/null
for replica in "${replicas[@]}"; do wait_for_value "$replica" docker-smoke 7,8,9; done

repair=${followers[3]}
"${compose[@]}" stop "$repair"
run_client "$elected" put 5000 one-follower-down 1 >/dev/null
"${compose[@]}" start "$repair"
for _ in {1..80}; do
    if run_client "$repair" health 5000 >/dev/null 2>&1; then break; fi
    sleep 0.1
done
run_client "$elected" put 5000 repair-trigger 4 >/dev/null
wait_for_value "$repair" docker-smoke 7,8,9
wait_for_value "$repair" one-follower-down 1

"${compose[@]}" stop "${followers[0]}" "${followers[1]}"
run_client "$elected" put 5000 repaired-follower-quorum 5 >/dev/null
"${compose[@]}" stop "$repair"
if run_client "$elected" put 5000 no-quorum 2; then
    echo "FAIL: PUT succeeded with fewer than three replicas" >&2
    exit 1
fi

echo "Five-replica Docker smoke test passed (quorum 3)"
