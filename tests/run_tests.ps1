# ============================================================================
# tests/run_tests.ps1 - optional wrapper: runs the Gate-1 tests T01..T07.
# The tests themselves are implemented natively in controller.exe
# ("controller.exe test"), so they use exactly the same Windows process
# management as production code. Expected last line: PASS=7 FAIL=0
# ============================================================================
$root = Split-Path -Parent $PSScriptRoot
$ctl = $null
foreach ($c in 'build\Release\controller.exe', 'build\RelWithDebInfo\controller.exe',
               'build\Debug\controller.exe', 'build\controller.exe') {
    $p = Join-Path $root $c
    if (Test-Path $p) { $ctl = $p; break }
}
if (-not $ctl) {
    Write-Error "controller.exe not found. Build first: cmake -S . -B build; cmake --build build --config Release"
    exit 2
}
& $ctl test @args
exit $LASTEXITCODE
