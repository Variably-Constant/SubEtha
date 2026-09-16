# Shared by every suite: the module import, a scratch directory per
# suite, and the byte helpers. Dot-sourced from each suite's BeforeAll.

function Import-SubEthaModule {
    $module = $env:PWRS_MODULE
    if (-not $module) { throw 'PWRS_MODULE is not set' }
    Import-Module (Join-Path $module 'SubEtha.psd1') -Force -ErrorAction Stop
}

function New-SubEthaScratch {
    param([string] $Name)
    $dir = Join-Path ([System.IO.Path]::GetTempPath()) ('subetha-ps-' + $Name + '-' + [guid]::NewGuid().ToString('n'))
    New-Item -ItemType Directory -Path $dir | Out-Null
    $dir
}

function Remove-SubEthaScratch {
    param([string] $Dir)
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
    if (Test-Path $Dir) {
        Get-ChildItem -Path $Dir -Recurse -File | Remove-Item -Force -ErrorAction SilentlyContinue
        Get-ChildItem -Path $Dir -Directory | Remove-Item -Force -ErrorAction SilentlyContinue
        Remove-Item -Path $Dir -Force -ErrorAction SilentlyContinue
    }
}

function ConvertTo-SEBytes {
    # Written with NoEnumerate so the byte[] reaches the caller as one
    # array rather than being unrolled into an object[] of bytes.
    param([string] $Text)
    Write-Output -NoEnumerate ([byte[]] [System.Text.Encoding]::UTF8.GetBytes($Text))
}

function ConvertFrom-SEBytes {
    param([byte[]] $Bytes)
    [System.Text.Encoding]::UTF8.GetString($Bytes)
}

function Get-SEText {
    # The text of a byte[] the module returned, with any trailing zero
    # bytes of a fixed-size slot trimmed.
    param([byte[]] $Bytes)
    ([System.Text.Encoding]::UTF8.GetString($Bytes)).TrimEnd([char]0)
}
