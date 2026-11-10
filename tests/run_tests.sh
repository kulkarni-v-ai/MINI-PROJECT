#!/usr/bin/env bash
# ============================================================================
# tests/run_tests.sh - Gate-1 test scenarios T01..T07 (spec §12)
#
# Each test prints PASS or FAIL and a one-line reason.
# Run from anywhere:  bash tests/run_tests.sh
# Requires: built binaries (cmake --build build). The same scenarios also run
# natively (no bash) via:  build/controller test
# ============================================================================
set -u
cd "$(dirname "$0")/.."
ROOT=$(pwd)
CTL=./build/controller
mkdir -p run

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "PASS $1 - $2"; }
bad()  { FAIL=$((FAIL+1)); echo "FAIL $1 - $2"; }

fresh_system() {
    # stop any running peers, wipe state, start 3 clean peers (all through the
    # controller's native process management - same code path as on Windows)
    $CTL sys stop >/dev/null 2>&1
    sleep 0.3
    $CTL reset >/dev/null
    $CTL sys start >/dev/null || echo "warning: peers did not start cleanly"
    head -c 300000 /dev/urandom > input/test_input.bin
}

kill_peer() {  # kill_peer <id>
    $CTL fail "$1" >/dev/null
}

events_contain() { grep -q "$1" metadata/events.json; }

# ---------------------------------------------------------------- T01
t01() {
    fresh_system
    $CTL chunk input/test_input.bin >/dev/null || { bad T01 "chunking failed"; return; }
    $CTL distribute >/dev/null || { bad T01 "distribution failed"; return; }
    # round-robin with rf=2 over 5 chunks: A={C1,C3,C4} B={C1,C2,C4,C5} C={C2,C3,C5}
    local fid
    fid=$(grep -o '"file_id": *"F[0-9]*"' metadata/file_metadata.json | grep -o 'F[0-9]*')
    local a b c
    a=$(ls storage/peer_A/chunks | grep -c "$fid")
    b=$(ls storage/peer_B/chunks | grep -c "$fid")
    c=$(ls storage/peer_C/chunks | grep -c "$fid")
    [ "$a" = 3 ] && [ "$b" = 4 ] && [ "$c" = 3 ] \
        && ok T01 "round-robin placement A=3,B=4,C=3, replication factor 2" \
        || bad T01 "unexpected placement A=$a,B=$b,C=$c"
}

# ---------------------------------------------------------------- T02
t02() {
    fresh_system
    $CTL chunk input/test_input.bin >/dev/null && $CTL distribute >/dev/null
    kill_peer B
    $CTL detect >/dev/null 2>&1
    $CTL recover --failed B >/dev/null 2>&1 || { bad T02 "recovery failed"; return; }
    $CTL reconstruct >/dev/null 2>&1 \
        && cmp -s input/test_input.bin output/recovered_test_input.bin \
        && ok T02 "single peer failure: detected, recovered, byte-identical" \
        || bad T02 "reconstruction mismatch after peer B failure"
}

# ---------------------------------------------------------------- T03
t03() {
    fresh_system
    $CTL chunk input/test_input.bin >/dev/null && $CTL distribute >/dev/null
    kill_peer C
    $CTL detect >/dev/null 2>&1
    $CTL recover --failed C --simulate-interrupt >/dev/null 2>&1 \
        || { bad T03 "recovery failed"; return; }
    if events_contain TRANSFER_INTERRUPTED; then
        $CTL reconstruct >/dev/null 2>&1 \
            && cmp -s input/test_input.bin output/recovered_test_input.bin \
            && ok T03 "interrupted transfer discarded, full retry succeeded" \
            || bad T03 "reconstruction failed after retry"
    else
        bad T03 "no TRANSFER_INTERRUPTED event logged"
    fi
}

# ---------------------------------------------------------------- T04
t04() {
    fresh_system
    $CTL chunk input/test_input.bin >/dev/null && $CTL distribute >/dev/null
    local fid
    fid=$(grep -o '"file_id": *"F[0-9]*"' metadata/file_metadata.json | grep -o 'F[0-9]*')
    # corrupt the chunk copy on peer A (primary of chunk 1); B's copy stays intact
    printf 'CORRUPTED' | dd of="storage/peer_A/chunks/${fid}-C001.chunk" bs=1 count=9 conv=notrunc 2>/dev/null
    # no peer is killed: recover treats A as the "failed" peer for chunk
    # selection, probes find both holders alive; A serves a corrupt copy,
    # SHA-256 rejects it, B serves the valid copy (spec §12 step 11).
    $CTL recover --failed A >/dev/null 2>&1 || { bad T04 "recovery failed"; return; }
    if events_contain INTEGRITY_FAILURE; then
        $CTL reconstruct >/dev/null 2>&1 \
            && cmp -s input/test_input.bin output/recovered_test_input.bin \
            && ok T04 "SHA-256 mismatch rejected; recovered from second source" \
            || bad T04 "reconstruction failed after integrity rejection"
    else
        bad T04 "no INTEGRITY_FAILURE event logged"
    fi
}

# ---------------------------------------------------------------- T05
t05() {
    fresh_system
    $CTL chunk input/test_input.bin >/dev/null && $CTL distribute >/dev/null
    kill_peer A; kill_peer B
    $CTL detect --all >/dev/null 2>&1 &
    DETECT_PID=$!
    sleep 3; kill $DETECT_PID 2>/dev/null
    $CTL recover --failed A >/dev/null 2>&1
    rc=$?
    if [ $rc -eq 3 ] && events_contain UNRECOVERABLE; then
        # reconstruction must refuse: required chunks are missing
        if ! $CTL reconstruct >/dev/null 2>&1; then
            ok T05 "no surviving source -> UNRECOVERABLE reported, reconstruction refused"
        else
            bad T05 "reconstruction should have been refused"
        fi
    else
        bad T05 "expected UNRECOVERABLE (rc=$rc)"
    fi
}

# ---------------------------------------------------------------- T06
t06() {
    fresh_system
    $CTL chunk input/test_input.bin >/dev/null && $CTL distribute >/dev/null
    kill_peer A
    $CTL detect >/dev/null 2>&1
    $CTL recover --failed A >/dev/null 2>&1
    $CTL reconstruct >/dev/null 2>&1
    rc=$?
    if [ $rc -eq 0 ] && cmp -s input/test_input.bin output/recovered_test_input.bin \
       && events_contain '"byte_identical": true'; then
        ok T06 "reconstruction in sequence order, byte-identical to original"
    else
        bad T06 "reconstruction comparison failed (rc=$rc)"
    fi
}

# ---------------------------------------------------------------- T07
t07() {
    fresh_system
    $CTL chunk input/test_input.bin >/dev/null && $CTL distribute >/dev/null
    kill_peer B; kill_peer C
    $CTL detect --all >/dev/null 2>&1 &
    DETECT_PID=$!
    sleep 3; kill $DETECT_PID 2>/dev/null
    out=$($CTL recover --failed B 2>&1)
    rc=$?
    # With rf=2, chunks held by B+C (C2, C5) have no surviving source.
    # The system must keep working: recover what survives, explicitly
    # report the rest as UNRECOVERABLE (multiple-peer failure handling).
    if [ $rc -eq 3 ] && echo "$out" | grep -q UNRECOVERABLE \
       && echo "$out" | grep -q "recovered (3)"; then
        ok T07 "two-peer failure: surviving chunks recovered, rest UNRECOVERABLE"
    else
        bad T07 "unexpected result for two-peer failure (rc=$rc)"
    fi
}

echo "================ Gate-1 test suite ================"
t01; t02; t03; t04; t05; t06; t07
# cleanup
$CTL sys stop >/dev/null 2>&1
$CTL reset >/dev/null
echo "==================================================="
echo "PASS=$PASS FAIL=$FAIL"
[ $FAIL -eq 0 ]
