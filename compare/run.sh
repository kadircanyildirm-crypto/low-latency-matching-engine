#!/usr/bin/env bash
# Head-to-head comparison driver; see docs/COMPARISON.md. Run from anywhere:
#
#   compare/run.sh export          record the command streams into compare/data
#   compare/run.sh ours            one round of our engine over every scenario
#   compare/run.sh orderbook-rs    one round of OrderBook-rs (crates.io)
#   compare/run.sh all [ROUNDS]    ROUNDS interleaved rounds (default 5) of every engine,
#                                  into a fresh compare/results/results.csv, then the report
#   compare/run.sh report          summarise compare/results/results.csv
#
# Environment (see compare/harness/src/run.rs): CMP_SCENARIOS (comma-separated subset of
# baseline,sweep,deep,modify), CMP_RUNS (runs per process, default 1), CMP_CORE (core to
# pin to, default the last one), CMP_COMMANDS (measured commands per stream, for export).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
results="$here/results/results.csv"

# Every engine with an adapter, in the order of the first round.
ENGINES=(ours orderbook-rs)

cargo_cmp() {
    cargo "$1" --manifest-path "$here/Cargo.toml" "${@:2}"
}

run_engine() {
    case "$1" in
        ours) cargo_cmp run --release --quiet --bin run-ours ;;
        orderbook-rs) cargo_cmp run --release --quiet --bin run-orderbook-rs ;;
        *)
            echo "unknown engine: $1 (known: ${ENGINES[*]})" >&2
            exit 2
            ;;
    esac
}

all() {
    local rounds="${1:-5}"
    # Build first, so no compilation overlaps a measurement.
    cargo_cmp build --release --quiet --bins
    if [[ -s "$results" ]]; then
        mv "$results" "$here/results/results-$(date +%Y%m%d-%H%M%S).csv"
    fi
    local n=${#ENGINES[@]}
    for ((round = 1; round <= rounds; round++)); do
        # Rotate the order every round, so no engine always runs first or last.
        for ((i = 0; i < n; i++)); do
            CMP_ROUND="$round" run_engine "${ENGINES[$(((round - 1 + i) % n))]}"
        done
    done
    cargo_cmp run --release --quiet --bin report
}

case "${1:-}" in
    export) cargo_cmp run --release --quiet --bin export ;;
    all) all "${2:-5}" ;;
    report) cargo_cmp run --release --quiet --bin report ;;
    "" | -h | --help) awk 'NR > 1 && /^#/ { print } /^set / { exit }' "$0" ;;
    *) run_engine "$1" ;;
esac
