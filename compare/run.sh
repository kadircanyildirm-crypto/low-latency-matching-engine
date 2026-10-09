#!/usr/bin/env bash
# Head-to-head comparison driver; see docs/COMPARISON.md. Run from anywhere:
#
#   compare/run.sh fetch           clone the C++ and Java competitors at their pinned
#                                  commits into compare/vendor (git-ignored), plus a
#                                  portable JDK 17 and Maven unless JAVA_HOME and MVN are set
#   compare/run.sh export          record the command streams into compare/data
#   compare/run.sh ours            one round of our engine over every scenario
#   compare/run.sh orderbook-rs    one round of OrderBook-rs (crates.io)
#   compare/run.sh liquibook       one round of liquibook (C++; needs fetch)
#   compare/run.sh exchange-core   one round of exchange-core (Java; needs fetch)
#   compare/run.sh all [ROUNDS]    ROUNDS interleaved rounds (default 5) of every engine,
#                                  into a fresh compare/results/results.csv, then the report
#   compare/run.sh report          summarise compare/results/results.csv
#
# Environment (see compare/harness/src/run.rs): CMP_SCENARIOS (comma-separated subset of
# baseline,sweep,deep,modify), CMP_RUNS (runs per process, default 1), CMP_CORE (core to
# pin to, default the last one), CMP_COMMANDS (measured commands per stream, for export).
# JAVA_HOME and MVN pick a JDK 17 and Maven; JAVA_OPTS overrides the JVM options.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
vendor="$here/vendor"
results="$here/results/results.csv"

# Every engine with an adapter, in the order of the first round.
ENGINES=(ours orderbook-rs liquibook exchange-core)

# Competitors built from source, pinned to the commits the comparison used.
LIQUIBOOK_URL=https://github.com/enewhuis/liquibook.git
LIQUIBOOK_REV=2427613b32f1667abae68a01df6af9ba8270f8e7
EXCHANGE_CORE_URL=https://github.com/exchange-core/exchange-core.git
EXCHANGE_CORE_REV=2f8548749839e9095c8dc597e4b61521d259fa5d

# Portable build tools for exchange-core, used when JAVA_HOME / MVN are not set.
JDK_DIR="$vendor/tools/jdk-17.0.20.1+1"
JDK_BASE=https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.20.1%2B1
JDK_WINDOWS=OpenJDK17U-jdk_x64_windows_hotspot_17.0.20.1_1.zip
JDK_WINDOWS_SHA256=e53a79c3c3d86865bd7e787903884331068e71321714ffd44f145785affc7cb0
JDK_LINUX=OpenJDK17U-jdk_x64_linux_hotspot_17.0.20.1_1.tar.gz
JDK_LINUX_SHA256=3808d1d15e3ec6bd5b84057fb5d84c33d8a1536a258146bcea2e603fc726e08e
MAVEN_DIR="$vendor/tools/apache-maven-3.9.9"
MAVEN_URL=https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz
MAVEN_SHA512=a555254d6b53d267965a3404ecb14e53c3827c09c3b94b5678835887ab404556bfaf78dcfe03ba76fa2508649dca8531c74bca4d5846513522404d48e8c4ac8b

case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*) windows=1 ;;
    *) windows=0 ;;
esac

# A path as the JVM wants it: native on Windows.
native() {
    if ((windows)); then cygpath -w "$1"; else echo "$1"; fi
}

cargo_cmp() {
    cargo "$1" --manifest-path "$here/Cargo.toml" "${@:2}"
}

# clone_pinned NAME URL REV: a checkout of REV in compare/vendor/NAME.
clone_pinned() {
    local dir="$vendor/$1"
    if [[ ! -d "$dir/.git" ]]; then
        git clone --quiet "$2" "$dir"
    fi
    if ! git -C "$dir" cat-file -e "$3^{commit}" 2>/dev/null; then
        git -C "$dir" fetch --quiet origin
    fi
    git -C "$dir" checkout --quiet --detach "$3"
    echo "$1 at $(git -C "$dir" rev-parse HEAD)"
}

# download URL FILE SUM ALGO: FILE from URL, verified against its published checksum.
download() {
    curl -fsSL -o "$2" "$1"
    echo "$3  $2" | "${4}sum" -c --quiet -
}

fetch_java_tools() {
    if [[ -n "${JAVA_HOME:-}" && -n "${MVN:-}" ]]; then
        return
    fi
    local dl="$vendor/tools/dl"
    mkdir -p "$dl"
    if [[ ! -d "$JDK_DIR" ]]; then
        if ((windows)); then
            download "$JDK_BASE/$JDK_WINDOWS" "$dl/$JDK_WINDOWS" "$JDK_WINDOWS_SHA256" sha256
            unzip -q "$dl/$JDK_WINDOWS" -d "$vendor/tools"
        elif [[ "$(uname -s)-$(uname -m)" == Linux-x86_64 ]]; then
            download "$JDK_BASE/$JDK_LINUX" "$dl/$JDK_LINUX" "$JDK_LINUX_SHA256" sha256
            tar -xzf "$dl/$JDK_LINUX" -C "$vendor/tools"
        else
            echo "no portable JDK for this platform: set JAVA_HOME (JDK 17) and MVN" >&2
            exit 2
        fi
    fi
    if [[ ! -d "$MAVEN_DIR" ]]; then
        download "$MAVEN_URL" "$dl/maven.tar.gz" "$MAVEN_SHA512" sha512
        tar -xzf "$dl/maven.tar.gz" -C "$vendor/tools"
    fi
}

fetch() {
    mkdir -p "$vendor"
    clone_pinned liquibook "$LIQUIBOOK_URL" "$LIQUIBOOK_REV"
    clone_pinned exchange-core "$EXCHANGE_CORE_URL" "$EXCHANGE_CORE_REV"
    fetch_java_tools
}

# Builds exchange-core (main classes only: its default profile's delombok, javadoc and
# signing steps are skipped, and so are its tests), then the replay harness against it,
# and runs one round.
exchange_core() {
    export JAVA_HOME="${JAVA_HOME:-$JDK_DIR}"
    local mvn="${MVN:-$MAVEN_DIR/bin/mvn}"
    local src="$vendor/exchange-core"
    if [[ ! -d "$src" ]]; then
        echo "exchange-core sources not found in $src: run compare/run.sh fetch" >&2
        exit 2
    fi
    local repo pom
    repo="$(native "$vendor/m2")"
    pom="$(native "$src/pom.xml")"
    local jar="$src/target/exchange-core-0.5.4-SNAPSHOT.jar"
    local cpfile="$vendor/exchange-core.classpath"
    if [[ ! -f "$jar" ]]; then
        "$mvn" -B -q -T 2 -f "$pom" -P '!default' -Dmaven.test.skip=true \
            -Dmaven.repo.local="$repo" package
    fi
    if [[ ! -f "$cpfile" ]]; then
        "$mvn" -B -q -f "$pom" -P '!default' -Dmaven.repo.local="$repo" \
            dependency:build-classpath -Dmdep.includeScope=runtime \
            -Dmdep.outputFile="$(native "$cpfile")"
    fi
    local sep=":"
    if ((windows)); then sep=";"; fi
    local classes="$here/target/java"
    local cp
    cp="$(native "$classes")$sep$(native "$jar")$sep$(cat "$cpfile")"
    mkdir -p "$classes"
    "$JAVA_HOME/bin/javac" -nowarn -d "$(native "$classes")" -cp "$cp" \
        "$(native "$here/exchange-core/ExchangeCoreReplay.java")"
    # The parallel collector was the default of Java 8, exchange-core's reference platform.
    # shellcheck disable=SC2086
    CMP_DATA="$(native "${CMP_DATA:-$here/data}")" \
        CMP_RESULTS="$(native "${CMP_RESULTS:-$results}")" \
        "$JAVA_HOME/bin/java" ${JAVA_OPTS:--Xms1g -Xmx2g -XX:+UseParallelGC} -cp "$cp" \
        ExchangeCoreReplay
}

run_engine() {
    case "$1" in
        ours) cargo_cmp run --release --quiet --bin run-ours ;;
        orderbook-rs) cargo_cmp run --release --quiet --bin run-orderbook-rs ;;
        liquibook) cargo_cmp run --release --quiet --bin run-liquibook ;;
        exchange-core) exchange_core ;;
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
    fetch) fetch ;;
    export) cargo_cmp run --release --quiet --bin export ;;
    all) all "${2:-5}" ;;
    report) cargo_cmp run --release --quiet --bin report ;;
    "" | -h | --help) awk 'NR > 1 && /^#/ { print } /^set / { exit }' "$0" ;;
    *) run_engine "$1" ;;
esac
