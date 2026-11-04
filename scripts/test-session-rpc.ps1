$ErrorActionPreference = 'Stop'
$sdkRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$hostRoot = (Resolve-Path (Join-Path $sdkRoot '../remotemedia-sdk-base')).Path
$sdkTarget = Join-Path $sdkRoot 'target'
$oldTarget = Join-Path $sdkTarget 'abi-v04'
$savedOldDll = $env:ABI_V04_DLL
$savedCurrentDll = $env:SESSION_RPC_DLL
$savedPythonPath = $env:PYTHONPATH

function Invoke-Checked {
    $program = $args[0]
    $nativeArguments = $args[1..($args.Count - 1)]
    $executable = (Get-Command $program -ErrorAction Stop).Source
    # Windows PowerShell treats redirected native stderr as ErrorRecords.
    # Compiler warnings are not failures; the exit status is authoritative.
    $ErrorActionPreference = 'Continue'
    & $executable @nativeArguments
    if ($LASTEXITCODE -ne 0) { throw "$program failed with exit code $LASTEXITCODE" }
}

Push-Location ([System.IO.Path]::GetTempPath())
try {
    Invoke-Checked cargo test --manifest-path "$sdkRoot/Cargo.toml" --target-dir $sdkTarget -p remotemedia-plugin-sdk -p remotemedia-traits
    Invoke-Checked cargo build --manifest-path "$sdkRoot/examples/session-rpc/Cargo.toml" --target-dir $sdkTarget
    Invoke-Checked cargo build --manifest-path "$sdkRoot/crates/plugin-sdk/tests/fixtures/abi-v04/Cargo.toml" --target-dir $oldTarget
    $env:ABI_V04_DLL = Join-Path $oldTarget 'debug/abi_v04_fixture.dll'
    $env:SESSION_RPC_DLL = Join-Path $sdkTarget 'debug/session_rpc_example.dll'
    $probe = Join-Path $oldTarget 'debug/probe.exe'
    Invoke-Checked $probe $env:ABI_V04_DLL accept
    Invoke-Checked $probe $env:SESSION_RPC_DLL reject
    Invoke-Checked cargo test --manifest-path "$sdkRoot/Cargo.toml" --target-dir $sdkTarget -p remotemedia-plugin-sdk --test binary_compatibility '--' --ignored
    Invoke-Checked cargo test --manifest-path "$hostRoot/integration/session-rpc/Cargo.toml" --target-dir "$hostRoot/target" '--' --include-ignored
    Invoke-Checked node "$hostRoot/crates/transports/ffi/nodejs/scripts/test-rpc-subscription.cjs"
    $clientRoot = Join-Path $sdkRoot '../remotemedia-sdk'
    if (Test-Path -LiteralPath "$clientRoot/clients/python/remotemedia/control/tests/test_rpc_proxy.py") {
        $env:PYTHONPATH = (Resolve-Path "$clientRoot/clients/python").Path
        Invoke-Checked python -m pytest "$clientRoot/clients/python/remotemedia/control/tests/test_rpc_proxy.py" "$clientRoot/clients/python/remotemedia/control/tests/test_transport_split.py" -q
    }

}
finally {
    $env:ABI_V04_DLL = $savedOldDll
    $env:SESSION_RPC_DLL = $savedCurrentDll
    $env:PYTHONPATH = $savedPythonPath
    Pop-Location
}
