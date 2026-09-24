# Runs Pester over Path against the module folder at Module, printing
# each test as it finishes, and exits with the number of failed tests.
# PesterPath names a Pester module to import when the host's module path
# has none of version 5 or later.
param(
    [Parameter(Mandatory)] [string] $Module,
    [Parameter(Mandatory)] [string] $Path,
    [string] $PesterPath = ''
)

$ErrorActionPreference = 'Stop'
$env:PWRS_MODULE = $Module
if ($PesterPath) {
    Import-Module $PesterPath
} else {
    Import-Module Pester -MinimumVersion 5.0
}
$c = New-PesterConfiguration
$c.Run.Path = $Path
$c.Run.Exit = $true
$c.Output.Verbosity = 'Detailed'
Write-Output ("pester-detailed: pwsh {0}, Pester {1}, pid {2}" -f $PSVersionTable.PSVersion, (Get-Module Pester | Select-Object -First 1).Version, $PID)
Invoke-Pester -Configuration $c
