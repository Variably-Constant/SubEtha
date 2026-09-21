# Generates the complete PowerShell reference from the built module, so
# the wiki states what the module actually exports rather than what
# somebody remembered. Reads the module and its shipped help; writes one
# section per run.
#
#   pwsh -File tools/Export-Reference.ps1 -Section cmdlets -OutFile ../../wiki/content/docs/reference/subetha-pwrs/cmdlets.md
#   pwsh -File tools/Export-Reference.ps1 -Section classes -OutFile ../../wiki/content/docs/reference/subetha-pwrs/classes.md
#   pwsh -File tools/Export-Reference.ps1 -Section enums   -OutFile ../../wiki/content/docs/reference/subetha-pwrs/enums.md
#
# Run it against a merged module folder so the surface is the one that
# ships, not one platform's half of it.
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [ValidateSet('cmdlets', 'classes', 'enums')] [string] $Section,
    [Parameter(Mandatory)] [string] $OutFile,
    [string] $Module = "$PSScriptRoot/../../../target/pwrs/SubEtha"
)

$ErrorActionPreference = 'Stop'

Import-Module (Join-Path $Module 'SubEtha.psd1') -Force
$asm = [SubEtha.Atomic].Assembly

# A CLR type as it reads in the shell: SubEtha names bare, Nullable<T>
# as T with a note, everything else by its short name.
function Format-Type([type] $t) {
    if ($null -eq $t) { return '' }
    if ($t.IsGenericType -and $t.GetGenericTypeDefinition() -eq [System.Nullable``1]) {
        return (Format-Type $t.GetGenericArguments()[0]) + '?'
    }
    if ($t.IsArray) { return (Format-Type $t.GetElementType()) + '[]' }
    switch ($t.FullName) {
        'System.String'  { return 'string' }
        'System.Boolean' { return 'bool' }
        'System.Byte'    { return 'byte' }
        'System.UInt16'  { return 'ushort' }
        'System.UInt32'  { return 'uint' }
        'System.UInt64'  { return 'ulong' }
        'System.Int32'   { return 'int' }
        'System.Int64'   { return 'long' }
        'System.Double'  { return 'double' }
        'System.Object'  { return 'object' }
        'System.Void'    { return 'void' }
        'System.Management.Automation.SwitchParameter' { return 'switch' }
        'System.Management.Automation.ScriptBlock'     { return 'scriptblock' }
    }
    if ($t.FullName -like 'SubEtha.*') { return $t.FullName }
    return $t.Name
}

function Format-Cell([string] $s) {
    if ([string]::IsNullOrWhiteSpace($s)) { return '' }
    ($s -replace '\r?\n', ' ' -replace '\|', '\|').Trim()
}

$common = [System.Management.Automation.PSCmdlet]::CommonParameters +
          [System.Management.Automation.PSCmdlet]::OptionalCommonParameters

# Which family a cmdlet belongs to, keyed by its noun with the SubEtha
# prefix removed. The families are the ones the reference index already
# uses, so the two pages agree. Anything unmatched lands in Other and is
# named on stderr at the end of the run, which is how a noun added later
# gets noticed rather than quietly filed under a heading nobody chose.
$Families = [ordered]@{
    'The front door'             = 'Channel', 'WorkQueue', 'KvMap', 'AdaptiveQueue', 'QosPolicy'
    'Rings and channels'         = 'Ring', 'SpscRing', 'BroadcastRing', 'PubSub', 'MpscPool', 'MpmcGrid', 'LamportPair'
    'Rings that change themselves' = 'CapacityRing', 'LocaleRing'
    'Order'                      = 'ReorderWindow'
    'Shared state'               = 'Atomic', 'Cell', 'Vec', 'Slab', 'HashMap', 'BTreeMap', 'LinkedList', 'Deque',
                                   'Stack', 'Arena', 'Region', 'FrameRegion', 'BitVec', 'SharedArc', 'LazyValue',
                                   'HandleTable', 'Graph', 'Universal', 'Tower', 'TopologyMap'
    'State with a history'       = 'VersionChain', 'VersionedSlab', 'VersionedMap', 'LanedMap', 'Epochs', 'TimePointTile'
    'Coordination'               = 'RWLock', 'Semaphore', 'Condvar', 'Condition', 'OwnerLease', 'Heartbeat',
                                   'EpochBarrier', 'LeaderElection', 'HolderTable', 'FenceClock', 'NotifierSet'
    'Probabilistic'              = 'BloomFilter', 'BlockedBloomFilter', 'CountMinSketch', 'HyperLogLog', 'Histogram',
                                   'RateLimiter', 'Reservoir', 'TinyBloom', 'FineBloom', 'BloomSize', 'SketchSize', 'LruCache'
    'Clocks'                     = 'Clock', 'CausalClock'
    'Sensing'                    = 'SensSender', 'SensReceiver', 'LossKind', 'LossBursts', 'Timing',
                                   'RoundTripShape', 'Periodicity', 'Capacity', 'Forecast', 'PathChanges'
    'Bridges'                    = 'TcpBridgeClient', 'TcpBridgeServer', 'QuicBridgeClient', 'QuicBridgeServer',
                                   'SelfSignedCert', 'Transport'
    'The pipeline'               = 'Item'
}

function Get-Family([string] $cmdletName) {
    $noun = ($cmdletName -split '-', 2)[1] -replace '^SubEtha', ''
    foreach ($f in $Families.Keys) {
        if ($Families[$f] -contains $noun) { return $f }
    }
    return 'Other'
}

# The properties and methods of a type the surface hands back, so a
# cmdlet entry says what you get as well as what you pass.
function Get-MemberLines([type] $t) {
    $lines = New-Object System.Collections.Generic.List[string]
    if ($null -eq $t -or $t.FullName -notlike 'SubEtha.*') { return $lines }
    $props = @($t.GetProperties() | Where-Object { $_.DeclaringType -eq $t } | Sort-Object Name)
    if ($props.Count) {
        $lines.Add('| Property | Type |')
        $lines.Add('|---|---|')
        foreach ($p in $props) { $lines.Add("| ``$($p.Name)`` | ``$(Format-Type $p.PropertyType)`` |") }
        $lines.Add('')
    }
    $methods = @($t.GetMethods() |
                 Where-Object { $_.DeclaringType -eq $t -and -not $_.IsSpecialName } |
                 Sort-Object Name)
    if ($methods.Count) {
        $lines.Add('| Method | Answers |')
        $lines.Add('|---|---|')
        foreach ($m in $methods) {
            $margs = @($m.GetParameters() | ForEach-Object { "$(Format-Type $_.ParameterType) $($_.Name)" })
            $lines.Add("| ``$($m.Name)($($margs -join ', '))`` | ``$(Format-Type $m.ReturnType)`` |")
        }
        $lines.Add('')
    }
    return $lines
}

$out = New-Object System.Text.StringBuilder
function Emit([string] $line = '') { $null = $out.AppendLine($line) }

if ($Section -eq 'cmdlets') {
    Emit '---'
    Emit 'title: "Every cmdlet, in full"'
    Emit 'weight: 10'
    Emit '---'
    Emit ''
    Emit '# Every cmdlet, in full'
    Emit ''
    $cmds = @(Get-Command -Module SubEtha -CommandType Cmdlet | Sort-Object Name)
    Emit "Every one of the $($cmds.Count) cmdlets the module exports, with each"
    Emit 'parameter, its type, whether it is required, and what the cmdlet'
    Emit 'writes, followed by the properties and methods of whatever came'
    Emit 'back. Generated from the built module by'
    Emit '`crates/subetha-pwrs/tools/Export-Reference.ps1`, so it cannot'
    Emit 'disagree with the surface it describes.'
    Emit ''
    Emit 'For the shape of an actual returned value rather than its type,'
    Emit 'see [What the values look like](../values/). In a shell,'
    Emit '`Get-Help <name> -Full` adds an example to any of these.'
    Emit ''

    # Group before emitting, so the page has sections and a contents list
    # rather than 135 headings in one flat run.
    $grouped = [ordered]@{}
    foreach ($f in $Families.Keys) { $grouped[$f] = New-Object System.Collections.Generic.List[object] }
    $grouped['Other'] = New-Object System.Collections.Generic.List[object]
    foreach ($c in $cmds) { $grouped[(Get-Family $c.Name)].Add($c) }

    Emit '## Contents'
    Emit ''
    foreach ($f in $grouped.Keys) {
        $members = $grouped[$f]
        if ($members.Count -eq 0) { continue }
        $anchor = ($f.ToLower() -replace '[^a-z0-9]+', '-').Trim('-')
        $names = ($members | ForEach-Object {
            $a = ($_.Name.ToLower() -replace '[^a-z0-9]+', '-').Trim('-')
            "[``$($_.Name)``](#$a)"
        }) -join ', '
        Emit "**[$f](#$anchor)** ($($members.Count)) - $names"
        Emit ''
    }

    foreach ($f in $grouped.Keys) {
        $members = $grouped[$f]
        if ($members.Count -eq 0) { continue }
        Emit "## $f"
        Emit ''
        foreach ($c in $members) {
        $h = Get-Help $c.Name -Full -ErrorAction SilentlyContinue
        $alias = @(Get-Alias -Definition $c.Name -ErrorAction SilentlyContinue | ForEach-Object { $_.Name })
        Emit "### $($c.Name)"
        Emit ''
        $syn = Format-Cell "$($h.Synopsis)"
        if ($syn) { Emit $syn; Emit '' }
        if ($alias.Count) {
            $aliasTxt = ($alias | Sort-Object | ForEach-Object { '`' + $_ + '`' }) -join ', '
            Emit "Also $aliasTxt."
            Emit ''
        }

        $sets = @($c.ParameterSets)
        foreach ($set in $sets) {
            if ($sets.Count -gt 1) { Emit "**Parameter set `$($set.Name)`**"; Emit '' }
            $ps = @($set.Parameters | Where-Object { $_.Name -notin $common })
            if ($ps.Count -eq 0) { Emit '_No parameters._'; Emit ''; continue }
            Emit '| Parameter | Type | Required | Position | Pipeline |'
            Emit '|---|---|---|---|---|'
            foreach ($p in $ps) {
                $pos = if ($p.Position -ge 0) { $p.Position } else { 'named' }
                $pipe = @()
                if ($p.ValueFromPipeline) { $pipe += 'value' }
                if ($p.ValueFromPipelineByPropertyName) { $pipe += 'name' }
                $pipeTxt = if ($pipe.Count) { $pipe -join ', ' } else { '-' }
                $req = if ($p.IsMandatory) { 'yes' } else { '-' }
                Emit "| ``$($p.Name)`` | ``$(Format-Type $p.ParameterType)`` | $req | $pos | $pipeTxt |"
            }
            Emit ''
        }

        $outTypeObjs = @($c.OutputType | ForEach-Object { $_.Type } | Where-Object { $_ })
        $outTypes = @($outTypeObjs | ForEach-Object { Format-Type $_ } | Sort-Object -Unique)
        if ($outTypes.Count) {
            $outTxt = ($outTypes | ForEach-Object { '`' + $_ + '`' }) -join ', '
            Emit "**Writes** $outTxt."
            Emit ''
            # What you can reach on what came back, so the entry answers
            # both halves without sending the reader to another page.
            foreach ($ot in ($outTypeObjs | Sort-Object FullName -Unique)) {
                $memberLines = Get-MemberLines $ot
                if ($memberLines.Count -eq 0) { continue }
                if ($outTypes.Count -gt 1) { Emit "On the ``$(Format-Type $ot)``:"; Emit '' }
                foreach ($line in $memberLines) { Emit $line }
            }
        } else {
            Emit '**Writes** nothing.'
            Emit ''
        }
        }
    }

    if ($grouped['Other'].Count) {
        Write-Warning ("unfiled nouns, add them to `$Families: " +
                       (($grouped['Other'] | ForEach-Object { $_.Name }) -join ', '))
    }
}

if ($Section -eq 'classes') {
    Emit '---'
    Emit 'title: "Every object, in full"'
    Emit 'weight: 20'
    Emit '---'
    Emit ''
    Emit '# Every object, in full'
    Emit ''
    $types = @($asm.GetExportedTypes() |
               Where-Object { $_.IsClass -and $_.FullName -like 'SubEtha.*' -and -not $_.Name.EndsWith('Command') } |
               Sort-Object FullName)
    Emit "Every one of the $($types.Count) object types a cmdlet or a method"
    Emit 'can write, with each property and each method signature. Generated'
    Emit 'from the built module by'
    Emit '`crates/subetha-pwrs/tools/Export-Reference.ps1`.'
    Emit ''
    Emit 'A method that answers `?` may answer `$null`, which the surface'
    Emit 'uses for an ordinary absent answer rather than a fault.'
    Emit ''
    Emit '## Contents'
    Emit ''
    $toc = ($types | ForEach-Object {
        $a = ($_.FullName.ToLower() -replace '[^a-z0-9]+', '-').Trim('-')
        "[``$($_.FullName)``](#$a)"
    }) -join ', '
    Emit $toc
    Emit ''

    foreach ($t in $types) {
        Emit "## $($t.FullName)"
        Emit ''
        $props = @($t.GetProperties() | Where-Object { $_.DeclaringType -eq $t } | Sort-Object Name)
        if ($props.Count) {
            Emit '| Property | Type |'
            Emit '|---|---|'
            foreach ($p in $props) { Emit "| ``$($p.Name)`` | ``$(Format-Type $p.PropertyType)`` |" }
            Emit ''
        }
        $methods = @($t.GetMethods() |
                     Where-Object { $_.DeclaringType -eq $t -and -not $_.IsSpecialName } |
                     Sort-Object Name)
        if ($methods.Count) {
            Emit '| Method | Answers |'
            Emit '|---|---|'
            foreach ($m in $methods) {
                $sig = @($m.GetParameters() | ForEach-Object { "$(Format-Type $_.ParameterType) $($_.Name)" })
                Emit "| ``$($m.Name)($($sig -join ', '))`` | ``$(Format-Type $m.ReturnType)`` |"
            }
            Emit ''
        }
        if (-not $props.Count -and -not $methods.Count) { Emit '_Carries no members of its own._'; Emit '' }
    }
}

if ($Section -eq 'enums') {
    Emit '---'
    Emit 'title: "Every enum"'
    Emit 'weight: 30'
    Emit '---'
    Emit ''
    Emit '# Every enum'
    Emit ''
    $enums = @($asm.GetExportedTypes() | Where-Object { $_.IsEnum } | Sort-Object FullName)
    Emit "The $($enums.Count) enums the surface takes and answers. Each is"
    Emit 'accepted as its type or as its name in a string, and the string'
    Emit 'form reads the same in both hosts:'
    Emit ''
    Emit '```powershell'
    Emit "`$atomic.FetchAdd(1, [SubEtha.MemoryOrder]::Relaxed)"
    Emit "`$atomic.FetchAdd(1, 'Relaxed')"
    Emit '```'
    Emit ''
    Emit '| Enum | Values |'
    Emit '|---|---|'
    foreach ($e in $enums) {
        $names = [Enum]::GetNames($e)
        Emit "| ``$($e.FullName)`` | ``$($names -join '`, `')`` |"
    }
    Emit ''
}

$dir = Split-Path -Parent $OutFile
if ($dir -and -not (Test-Path $dir)) { $null = New-Item -ItemType Directory -Force -Path $dir }
$text = $out.ToString()
[System.IO.File]::WriteAllText($OutFile, $text)
$lines = ($text -split "`n").Count
Write-Output "wrote $Section to $OutFile ($lines lines)"
