# Runs real scenarios against the built module and writes down what they
# actually answered, so the reference shows the shape of a value rather
# than only its type name. Nothing here is transcribed by hand: every
# output below the code is captured from the run that produced this page.
#
#   pwsh -File tools/Export-Examples.ps1 -OutFile ../../wiki/content/docs/reference/subetha-pwrs/values.md
#
# Scenarios are deliberately small and local. Each one works in its own
# directory under the system temp folder and disposes what it made.
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $OutFile,
    [string] $Module = "$PSScriptRoot/../../../target/pwrs/SubEtha"
)

$ErrorActionPreference = 'Stop'
Import-Module (Join-Path $Module 'SubEtha.psd1') -Force

$root = Join-Path ([System.IO.Path]::GetTempPath()) ("subetha-values-" + [guid]::NewGuid().ToString('N').Substring(0, 8))
$null = New-Item -ItemType Directory -Force -Path $root

# How a captured value is written down. A byte array shows its length and
# its leading bytes, because that is the thing a reader cannot guess; an
# object shows each property and what it held; everything else prints.
function Show-Value($v, [int] $depth = 0) {
    if ($null -eq $v) { return '$null' }
    $pad = ' ' * $depth
    if ($v -is [byte[]]) {
        $head = ($v | Select-Object -First 16) -join ', '
        $tail = if ($v.Length -gt 16) { ', ...' } else { '' }
        return "byte[$($v.Length)] : $head$tail"
    }
    if ($v -is [string]) { return "'$v'" }
    if ($v -is [bool]) { return "`$$($v.ToString().ToLower())" }
    if ($v.GetType().IsPrimitive) { return "$v" }
    if ($v.GetType().IsEnum) { return "$v  [$($v.GetType().FullName)]" }
    if ($v -is [System.Array] -or $v -is [System.Collections.IEnumerable]) {
        $items = @($v)
        if ($items.Count -eq 0) { return '@()  (empty)' }
        $lines = @("$($v.GetType().Name), $($items.Count) item(s):")
        foreach ($i in ($items | Select-Object -First 4)) { $lines += "$pad  " + (Show-Value $i ($depth + 2)) }
        if ($items.Count -gt 4) { $lines += "$pad  ..." }
        return ($lines -join "`n")
    }
    $t = $v.GetType()
    if ($t.FullName -like 'SubEtha.*') {
        $props = @($t.GetProperties() | Where-Object { $_.DeclaringType -eq $t })
        if ($props.Count) {
            $lines = @("[$($t.FullName)]")
            foreach ($p in $props) {
                $pv = try { $p.GetValue($v) } catch { '<threw>' }
                $lines += "$pad  $($p.Name) = " + (Show-Value $pv ($depth + 2))
            }
            return ($lines -join "`n")
        }
        return "[$($t.FullName)]"
    }
    return "$v"
}

$sections = New-Object System.Collections.Generic.List[object]
function Scenario([string] $group, [string] $title, [string] $code) {
    $sb = [scriptblock]::Create($code)
    $captured = & $sb
    $sections.Add([pscustomobject]@{ Group = $group; Title = $title; Code = $code.Trim(); Output = $captured })
}

# --- Shared state -------------------------------------------------------
Scenario 'Shared state' 'An atomic, and what each operation answers' @"
`$a = New-SubEthaAtomic -Path '$root\atom' -Init 10
@(
  "Store(40)            -> " + (Show-Value `$a.Store(40))
  "FetchAdd(2)          -> " + (Show-Value `$a.FetchAdd(2))
  "Load()               -> " + (Show-Value `$a.Load())
  "Swap(99)             -> " + (Show-Value `$a.Swap(99))
  "CompareExchange(99,1)-> " + (Show-Value `$a.CompareExchange(99, 1))
  "Load()               -> " + (Show-Value `$a.Load())
  "Path                 -> " + (Show-Value `$a.Path)
)
`$a.Dispose()
"@

Scenario 'Shared state' 'A map: keys and values are byte[] of the declared size' @"
`$m = New-SubEthaHashMap -Path '$root\map' -Capacity 64 -KeySize 8 -ValueSize 8
`$k = [BitConverter]::GetBytes([ulong]7)
`$v = [BitConverter]::GetBytes([ulong]70)
`$other = [BitConverter]::GetBytes([ulong]999)
@(
  "KeySize / ValueSize  -> " + (Show-Value `$m.KeySize) + ' / ' + (Show-Value `$m.ValueSize)
  "Insert(k, v)         -> " + (Show-Value `$m.Insert(`$k, `$v))
  "Insert(k, v) again   -> " + (Show-Value `$m.Insert(`$k, `$v))
  "Get(k)               -> " + (Show-Value `$m.Get(`$k))
  "Get(absent)          -> " + (Show-Value `$m.Get(`$other))
  "Contains(k)          -> " + (Show-Value `$m.Contains(`$k))
  "Count()              -> " + (Show-Value `$m.Count())
  "Remove(k)            -> " + (Show-Value `$m.Remove(`$k))
  "Get(k) after remove  -> " + (Show-Value `$m.Get(`$k))
)
`$m.Dispose()
"@

# --- Rings --------------------------------------------------------------
Scenario 'Rings' 'A ring: what Recv actually hands back' @"
`$r = New-SubEthaBroadcastRing -Path '$root\bcast' -Capacity 8
`$id = `$r.RegisterConsumer()
`$null = `$r.Push('hello')
@(
  "PayloadSize          -> " + (Show-Value `$r.PayloadSize)
  "Recv(id)             -> " + (Show-Value `$r.Recv(`$id))
  "Recv(id) when empty  -> " + (Show-Value `$r.Recv(`$id))
  "ProducerPosition()   -> " + (Show-Value `$r.ProducerPosition())
  "ActiveConsumers()    -> " + (Show-Value `$r.ActiveConsumers())
)
`$r.UnregisterConsumer(`$id)
`$r.Dispose()
"@

Scenario 'Rings' 'A full ring, and an empty one' @"
`$s = New-SubEthaSpscRing -Path '$root\spsc' -Capacity 4
`$pushed = for (`$i = 0; `$i -lt 6; `$i++) { `$s.Push([byte[]]@(`$i)) }
@(
  "Capacity             -> " + (Show-Value `$s.Capacity)
  "six Push calls       -> " + (Show-Value `$pushed)
  "Pop()                -> " + (Show-Value `$s.Pop())
)
`$s.Dispose()
"@

# --- Versioned ----------------------------------------------------------
Scenario 'Versioned' 'A pin, and the entries a scan answers' @"
`$vm = New-SubEthaVersionedMap -Path '$root\vmap' -Capacity 64 -EpochsPath '$root\vepochs'
`$null = `$vm.Insert(7, 70)
`$null = `$vm.Insert(9, 90)
`$pin = `$vm.Pin()
@(
  "Pin()                -> " + (Show-Value `$pin)
  "pin.Epoch()          -> " + (Show-Value `$pin.Epoch())
  "pin.Get(7)           -> " + (Show-Value `$pin.Get(7))
  "pin.Scan(0,100,10)   -> " + (Show-Value `$pin.Scan(0, 100, 10))
  "pin.ScanFrom(0,100,1)-> " + (Show-Value `$pin.ScanFrom(0, 100, 1))
)
`$pin.Release()
`$vm.Dispose()
"@

# --- Coordination -------------------------------------------------------
Scenario 'Coordination' 'A lock hold, and what a refused one looks like' @"
`$l = New-SubEthaRWLock -Path '$root\lock'
`$w = `$l.Write()
@(
  "Write()              -> " + (Show-Value `$w)
  "Readers()            -> " + (Show-Value `$l.Readers())
  "TryWrite() held      -> " + (Show-Value `$l.TryWrite())
  "WriteFor(0.2) held   -> " + (Show-Value `$l.WriteFor(0.2))
)
`$w.Release()
`$l.Dispose()
"@

# --- Probabilistic ------------------------------------------------------
Scenario 'Probabilistic' 'Sizing a filter and a sketch' @"
@(
  "Measure-SubEthaBloomSize -Items 10000 -FalsePositiveRate 0.01"
  "  -> " + (Show-Value (Measure-SubEthaBloomSize -Items 10000 -FalsePositiveRate 0.01))
  "Measure-SubEthaSketchSize -Epsilon 0.01 -Delta 0.01"
  "  -> " + (Show-Value (Measure-SubEthaSketchSize -Epsilon 0.01 -Delta 0.01))
)
"@

Scenario 'Probabilistic' 'A bloom filter answering about membership' @"
`$sz = Measure-SubEthaBloomSize -Items 1000 -FalsePositiveRate 0.01
`$b = New-SubEthaBloomFilter -Path '$root\bloom' -Bits `$sz.Bits -Hashes `$sz.Hashes
`$null = `$b.Insert('alice')
@(
  "sizing               -> Bits " + (Show-Value `$sz.Bits) + ', Hashes ' + (Show-Value `$sz.Hashes)
  "Insert('alice')      -> (an item is a byte[] or a string, never a number)"
  "Contains('alice')    -> " + (Show-Value `$b.Contains('alice'))
  "Contains('bob')      -> " + (Show-Value `$b.Contains('bob'))
)
`$b.Dispose()
"@

# --- Transports ---------------------------------------------------------
Scenario 'Transports' 'What the module reports and what a certificate is' @"
`$cert = New-SubEthaSelfSignedCert -Name 'subetha-example'
@(
  "Get-SubEthaTransport -> " + (Show-Value (Get-SubEthaTransport))
  "New-SubEthaSelfSignedCert -Name 'subetha-example'"
  "  -> " + (Show-Value `$cert)
)
"@

# --- Emit ---------------------------------------------------------------
$out = New-Object System.Text.StringBuilder
function Emit([string] $s = '') { $null = $out.AppendLine($s) }

# The scratch root out of anything bound for the page. It carries the
# temp directory and so the operator's home directory, and it changes
# every run, so leaving it in publishes a path off this machine and
# churns the diff besides.
function Redact([string] $s) { $s -replace [regex]::Escape($root), 'C:\ipc' }

Emit '---'
Emit 'title: "What the values look like"'
Emit 'weight: 5'
Emit '---'
Emit ''
Emit '# What the values look like'
Emit ''
Emit 'A type name says what comes back; it does not say what it looks like.'
Emit 'Every block below was run against the built module and the output is'
Emit 'what it answered, captured rather than written down. Generated by'
Emit '`crates/subetha-pwrs/tools/Export-Examples.ps1`.'
Emit ''
Emit 'A `byte[]` is shown as its length and its leading bytes, because the'
Emit 'length is the part a reader cannot guess: a ring hands back a whole'
Emit 'slot, payload then zeros, rather than only what was pushed.'
Emit ''

$groups = $sections | Group-Object Group
foreach ($g in $groups) {
    Emit "## $($g.Name)"
    Emit ''
    foreach ($s in $g.Group) {
        Emit "### $($s.Title)"
        Emit ''
        Emit '```powershell'
        # The scenarios work in a throwaway directory whose name changes
        # every run. Showing that would make the page look unreproducible
        # and would churn the diff, so the code reads with an ordinary
        # path. The answers need the same substitution and for a second
        # reason: a Path property hands back where the structure actually
        # is, which on a developer's machine is a home directory.
        foreach ($line in ($s.Code -split "`r?`n")) { Emit (Redact $line) }
        Emit '```'
        Emit ''
        Emit 'Answers:'
        Emit ''
        Emit '```'
        foreach ($line in @($s.Output)) {
            foreach ($sub in ("$line" -split "`r?`n")) { Emit (Redact $sub) }
        }
        Emit '```'
        Emit ''
    }
}

$dir = Split-Path -Parent $OutFile
if ($dir -and -not (Test-Path $dir)) { $null = New-Item -ItemType Directory -Force -Path $dir }
[System.IO.File]::WriteAllText($OutFile, $out.ToString())
$lines = ($out.ToString() -split "`n").Count
Write-Output "wrote $($sections.Count) scenarios to $OutFile ($lines lines)"
Write-Output "scratch left at $root"
