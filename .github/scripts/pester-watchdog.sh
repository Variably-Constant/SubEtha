#!/usr/bin/env bash
# Runs the module's Pester suite in one pwsh, printing each test as it
# finishes. A suite still running when the limit runs out has every
# thread of its pwsh sampled and its UDP sockets listed, and is killed.
# The test file it was in then runs again by itself under the same
# limit, which says whether that file stops on its own or only after
# the files before it have run in the same process.
#
# Usage: pester-watchdog.sh <module folder> <tests folder> <limit in seconds> [<Pester module path>]
# Exits with the suite's pwsh exit code, or 124 when the limit killed it.
set -u

module=$1
tests=$2
limit=$3
pester=${4:-}
here=$(cd "$(dirname "$0")" && pwd)
logs=$(mktemp -d)
# The module records what the suite exercised while this is set, as it
# does under `cargo pwrs test`.
export PWRS_SURFACE_DIR="$logs/surface"

# run <label> <path> <log>: the pwsh exit code, or 124 once the limit
# has run out and the pwsh has been killed.
run() {
    local label=$1 path=$2 log=$3
    : > "$log"
    local args=(-NoProfile -NonInteractive -File "$here/pester-detailed.ps1" -Module "$module" -Path "$path")
    if [ -n "$pester" ]; then
        args+=(-PesterPath "$pester")
    fi
    pwsh "${args[@]}" > "$log" 2>&1 &
    local pid=$!
    tail -n +1 -f "$log" &
    local tailer=$!
    local start=$SECONDS
    echo "=== $(date -u +%T) $label: pwsh pid $pid, limit ${limit}s"
    while kill -0 "$pid" 2> /dev/null; do
        if [ $((SECONDS - start)) -ge "$limit" ]; then
            echo "=== $(date -u +%T) $label: still running after ${limit}s"
            echo "=== threads of pid $pid"
            if command -v sample > /dev/null 2>&1; then
                if sudo sample "$pid" 5 -file "$log.stacks" > /dev/null 2>&1; then
                    cat "$log.stacks"
                else
                    echo "=== sample could not read pid $pid"
                fi
            else
                echo "=== this host has no sample"
            fi
            echo "=== UDP sockets of pid $pid"
            lsof -nP -a -p "$pid" -i UDP 2>&1 || true
            kill -9 "$pid" 2> /dev/null
            wait "$pid" 2> /dev/null
            kill "$tailer" 2> /dev/null
            wait "$tailer" 2> /dev/null
            return 124
        fi
        sleep 1
    done
    wait "$pid"
    local rc=$?
    sleep 1
    kill "$tailer" 2> /dev/null
    wait "$tailer" 2> /dev/null
    echo "=== $(date -u +%T) $label: exit $rc after $((SECONDS - start))s"
    return "$rc"
}

run "the suite" "$tests" "$logs/suite.log"
rc=$?
if [ "$rc" -eq 124 ]; then
    stuck=$(grep -oE "/[^' ]+\.Tests\.ps1" "$logs/suite.log" | tail -n 1)
    if [ -n "$stuck" ]; then
        run "$(basename "$stuck") by itself" "$stuck" "$logs/alone.log"
    else
        echo "=== the suite's output names no test file"
    fi
fi
exit "$rc"
