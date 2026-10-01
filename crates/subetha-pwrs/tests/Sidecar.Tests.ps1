# The process's sidecar: a managed ring runs a sidecar of its own that
# resizes or moves it, an observed object has the process's sidecar drain
# what it records into stats, and a ScriptBlock policy decides, on the
# sidecar's own thread, which tag an object runs at. A test waiting on a
# sidecar thread polls up to a generous deadline, so a busy machine slows
# it rather than failing it; Invoke-SubEthaSidecarScan is called wherever
# what was recorded must be counted before a test goes on.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'sidecar'

    function Wait-SEUntil {
        # Whether Check came true before the deadline, polling for it.
        param([scriptblock] $Check, [double] $Seconds = 10)
        $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
        while ([DateTime]::UtcNow -lt $deadline) {
            if (& $Check) { return $true }
            Start-Sleep -Milliseconds 5
        }
        [bool] (& $Check)
    }

    function New-SEObservableSet {
        # One object of every class that can be observed, each built small.
        param([string] $Dir)
        $heartbeat = New-SubEthaHeartbeat -Path (Join-Path $Dir 'heartbeat') -Capacity 4
        [ordered] @{
            Adaptive           = New-SubEthaAdaptive
            Arena              = New-SubEthaArena -Path (Join-Path $Dir 'arena') -CapacityBytes 4096
            Atomic             = New-SubEthaAtomic -Path (Join-Path $Dir 'atomic')
            BitVec             = New-SubEthaBitVec -Path (Join-Path $Dir 'bits') -CapacityBits 64
            BlockedBloomFilter = New-SubEthaBlockedBloomFilter -Path (Join-Path $Dir 'blocked') -Bits 1024 -Hashes 3
            BloomFilter        = New-SubEthaBloomFilter -Path (Join-Path $Dir 'bloom') -Bits 1024 -Hashes 3
            BroadcastRing      = New-SubEthaBroadcastRing -Path (Join-Path $Dir 'broadcast') -Capacity 8
            CountMinSketch     = New-SubEthaCountMinSketch -Path (Join-Path $Dir 'sketch') -Depth 4 -Width 64
            EpochBarrier       = New-SubEthaEpochBarrier -Path (Join-Path $Dir 'barrier') -HeartbeatPath (Join-Path $Dir 'heartbeat') -Capacity 4
            FenceClock         = New-SubEthaFenceClock -Path (Join-Path $Dir 'clock') -Capacity 4
            Graph              = New-SubEthaGraph -Path (Join-Path $Dir 'graph') -MaxNodes 8 -MaxEdges 16
            HandleTable        = New-SubEthaHandleTable -Path (Join-Path $Dir 'handles') -Capacity 4
            HashMap            = New-SubEthaHashMap -Path (Join-Path $Dir 'map') -Capacity 8 -KeySize 4 -ValueSize 4
            Heartbeat          = $heartbeat
            Histogram          = New-SubEthaHistogram -Path (Join-Path $Dir 'histogram') -Boundaries @(10, 100)
            HyperLogLog        = New-SubEthaHyperLogLog -Path (Join-Path $Dir 'hll')
            LeaderElection     = New-SubEthaLeaderElection -Path (Join-Path $Dir 'leader')
            OwnerLease         = New-SubEthaOwnerLease -Path (Join-Path $Dir 'lease') -Value 'v'
            RWLock             = New-SubEthaRWLock -Path (Join-Path $Dir 'rwlock')
            RateLimiter        = New-SubEthaRateLimiter -Path (Join-Path $Dir 'rate') -Capacity 4 -RefillPerSecond 2
            Reservoir          = New-SubEthaReservoir -Path (Join-Path $Dir 'reservoir') -Capacity 4
            Ring               = New-SubEthaRing -Path (Join-Path $Dir 'ring') -Capacity 8
            Semaphore          = New-SubEthaSemaphore -Path (Join-Path $Dir 'semaphore') -Initial 1
            TimePointTile      = New-SubEthaTimePointTile -Path (Join-Path $Dir 'tile')
            TopologyMap        = New-SubEthaTopologyMap -Path (Join-Path $Dir 'topology') -Participants 4
            Universal          = New-SubEthaUniversal -Path (Join-Path $Dir 'universal') -Capacity 8
            VersionChain       = New-SubEthaVersionChain -Path (Join-Path $Dir 'chain') -Capacity 4
        }
    }

    function Invoke-SEInRunspaces {
        # Runs Script in Count runspaces of a pool at once, each handed
        # Argument and a barrier that lets them all go together, and
        # returns everything they wrote. The module is imported in each.
        param([int] $Count, [string] $Script, $Argument)
        $state = [initialsessionstate]::CreateDefault()
        $state.ImportPSModule([string[]] @(Join-Path $env:PWRS_MODULE 'SubEtha.psd1'))
        $barrier = New-Object System.Threading.Barrier $Count
        $pool = [runspacefactory]::CreateRunspacePool(1, $Count, $state, $Host)
        $pool.Open()
        try {
            $running = foreach ($i in 1..$Count) {
                $shell = [powershell]::Create()
                $shell.RunspacePool = $pool
                $null = $shell.AddScript($Script).AddArgument($Argument).AddArgument($barrier)
                [pscustomobject] @{ Shell = $shell; Handle = $shell.BeginInvoke() }
            }
            foreach ($run in $running) {
                $run.Shell.EndInvoke($run.Handle)
                $run.Shell.Dispose()
            }
        } finally {
            $pool.Dispose()
            $barrier.Dispose()
        }
    }
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'A managed ring' {
    It 'runs a shape sidecar when managed and none when strict' {
        $path = Join-Path $script:dir 'managed'
        $managed = New-SubEthaRing -Path $path -Capacity 64 -Managed
        $managed.Managed | Should -BeTrue -Because "-Managed starts the ring's shape sidecar"
        $opened = Open-SubEthaRing -Path $path -Capacity 64 -Managed -ScanIntervalUs 500
        $opened.Managed | Should -BeTrue -Because 'an attaching handle runs a sidecar of its own'
        $strict = New-SubEthaRing -Path (Join-Path $script:dir 'strict') -Capacity 64
        $strict.Managed | Should -BeFalse
        $strict.SidecarMorphs() | Should -Be 0 -Because 'a strict ring has no sidecar to morph it'
        $opened.Dispose()
        $managed.Dispose()
        $strict.Dispose()
    }

    It 'refuses an interval without -Managed, and an interval of zero' {
        { New-SubEthaRing -Path (Join-Path $script:dir 'stray') -Capacity 64 -ScanIntervalUs 250 -ErrorAction Stop } |
            Should -Throw '*pass -Managed with it*'
        { New-SubEthaCapacityRing -Path (Join-Path $script:dir 'stray-cap') -Capacity 64 -ScanIntervalUs 250 -ErrorAction Stop } |
            Should -Throw '*pass -Managed with it*'
        { New-SubEthaRing -Path (Join-Path $script:dir 'zero') -Capacity 64 -Managed -ScanIntervalUs 0 -ErrorAction Stop } |
            Should -Throw '*at least one microsecond*'
    }

    It 'needs an interval when it is a capacity or locale ring' {
        { New-SubEthaCapacityRing -Path (Join-Path $script:dir 'cap') -Capacity 64 -Managed -ErrorAction Stop } |
            Should -Throw '*the library names no default*'
        { New-SubEthaLocaleRing -Path (Join-Path $script:dir 'loc') -Capacity 64 -Managed -ErrorAction Stop } |
            Should -Throw '*the library names no default*'
    }

    It 'grows once it fills when it is a managed capacity ring' {
        $ring = New-SubEthaCapacityRing -Path (Join-Path $script:dir 'grow') -Capacity 64 -Managed -ScanIntervalUs 1000
        $ring.Managed | Should -BeTrue
        $producer = $ring.RegisterProducer()
        $null = $ring.RegisterConsumer()
        $sent = $ring.SendMany($producer, [object[]] (@('x') * 64))
        $sent | Should -BeGreaterOrEqual 55 -Because 'the ring is filled past the 85 percent it grows at'
        Wait-SEUntil { $ring.Capacity() -gt 64 } | Should -BeTrue -Because 'the sidecar never grew a full ring'
        $ring.SidecarMorphs() | Should -BeGreaterOrEqual 1
        Wait-SEUntil { $ring.SidecarPrewarms() -ge 1 } |
            Should -BeTrue -Because 'a fill ratio held across scans has the sidecar build the next backing early'
        $ring.Dispose()
    }

    It 'changes size only when told when it is a strict capacity ring' {
        $ring = New-SubEthaCapacityRing -Path (Join-Path $script:dir 'strict-grow') -Capacity 64
        $ring.Managed | Should -BeFalse
        $producer = $ring.RegisterProducer()
        $null = $ring.RegisterConsumer()
        $null = $ring.SendMany($producer, [object[]] (@('x') * 64))
        # Five times the 100 ms a capacity sidecar waits between resizes:
        # long enough for one to have grown the ring, had one been running.
        Start-Sleep -Milliseconds 500
        $ring.Capacity() | Should -Be 64
        $ring.SidecarMorphs() | Should -Be 0
        $ring.SidecarPrewarms() | Should -Be 0
        $ring.Dispose()
    }

    It 'moves where it is asked when it is a managed locale ring' {
        $ring = New-SubEthaLocaleRing -Path (Join-Path $script:dir 'ask') -Capacity 64 -Managed -ScanIntervalUs 1000
        $ring.Managed | Should -BeTrue
        $ring.Locale() | Should -Be ([SubEtha.Locale]::Anon)
        $ring.RequestLocale('File')
        Wait-SEUntil { $ring.Locale() -eq [SubEtha.Locale]::File } | Should -BeTrue -Because 'the sidecar never moved the ring'
        $ring.SidecarMigrations() | Should -BeGreaterOrEqual 1
        $ring.Dispose()
    }

    It 'has no sidecar to ask when it is a strict locale ring' {
        $ring = New-SubEthaLocaleRing -Path (Join-Path $script:dir 'strict-ask') -Capacity 64
        $ring.Managed | Should -BeFalse
        { $ring.RequestLocale('File') } | Should -Throw '*a strict ring moves with MigrateTo*'
        $ring.SidecarMigrations() | Should -Be 0
        $ring.Dispose()
    }

    It 'stays where MigrateTo moved it' {
        $ring = New-SubEthaLocaleRing -Path (Join-Path $script:dir 'stay') -Capacity 64 -Managed -ScanIntervalUs 1000
        $ring.MigrateTo('File')
        # Four times the 250 ms the sidecar waits between moves: long enough
        # for it to move the ring back, had it been left asking for Anon.
        Start-Sleep -Seconds 1
        $ring.Locale() | Should -Be ([SubEtha.Locale]::File) -Because "the ring's own sidecar moved it back"
        $ring.SidecarMigrations() | Should -Be 0
        $ring.Dispose()
    }
}

Describe 'An observed object' {
    It 'is any object of a class that carries an observation ring' {
        $objects = New-SEObservableSet $script:dir
        $observable = [SubEtha.Registration].Assembly.GetExportedTypes() |
            Where-Object { $_.GetMethods() | Where-Object { $_.Name -eq 'Observe' -and $_.GetParameters().Name -contains 'policy' } } |
            ForEach-Object { $_.Name } |
            Sort-Object
        (@($objects.Keys | Sort-Object) -join ', ') |
            Should -Be ($observable -join ', ') -Because 'the table here covers every class that registers'
        foreach ($name in $objects.Keys) {
            $registration = $objects[$name].Observe()
            try {
                $registration.Closed() | Should -BeFalse -Because $name
                $registration.Stats().MigrationsTriggered | Should -Be 0 -Because $name
            } finally {
                $registration.Close()
            }
        }
    }

    It 'has what is done to it counted by kind' {
        $table = New-SubEthaHashMap -Path (Join-Path $script:dir 'table') -Capacity 64 -KeySize 4 -ValueSize 8
        $registration = $table.Observe()
        try {
            $null = $table.Insert((ConvertTo-SEBytes 'key1'), (ConvertTo-SEBytes 'value001'))
            $null = $table.Get((ConvertTo-SEBytes 'key1'))
            $null = $table.Get((ConvertTo-SEBytes 'none'))
            Invoke-SubEthaSidecarScan
            $stats = $registration.Stats()
        } finally {
            $registration.Close()
        }
        $stats | Should -BeOfType [SubEtha.InstanceStats]
        $stats.OpsObserved | Should -Be 3
        $kinds = $stats.OpKindCounts()
        $kinds.Count | Should -Be $stats.NOpKinds()
        $kinds[1] | Should -Be 1 -Because "one insert, the map's op kind 1"
        $kinds[2] | Should -Be 2 -Because 'two gets, its op kind 2'
        $stats.OpKindTotal() | Should -Be 3
        [Math]::Abs($stats.RatioOf(2, @(1, 2)) - 2 / 3) | Should -BeLessThan 1e-9
        $stats.MigrationsTriggered | Should -Be 0 -Because "a map's own policy never migrates it"
    }

    It 'leaves the sidecar when its registration closes' {
        $table = New-SubEthaHashMap -Path (Join-Path $script:dir 'closing') -Capacity 64 -KeySize 4 -ValueSize 8
        [GC]::Collect()
        [GC]::WaitForPendingFinalizers()
        $before = (Get-SubEthaSidecar).InstanceCount
        $registration = $table.Observe()
        (Get-SubEthaSidecar).InstanceCount | Should -Be ($before + 1)
        $registration.Tag() | Should -Be 0
        $registration.Close()
        $registration.Closed() | Should -BeTrue
        (Get-SubEthaSidecar).InstanceCount | Should -Be $before
        { $registration.Stats() } | Should -Throw '*the registration is closed*'
        { $registration.Tag() } | Should -Throw '*the registration is closed*'
        $registration.Close()
    }

    It 'leaves the sidecar when its registration is disposed' {
        $table = New-SubEthaHashMap -Path (Join-Path $script:dir 'disposing') -Capacity 64 -KeySize 4 -ValueSize 8
        $registration = $table.Observe()
        $registration.Dispose()
        $second = $table.Observe()
        $second.Closed() | Should -BeFalse -Because 'a disposed registration frees the object for another'
        $second.Close()
    }

    It 'has one registration at a time' {
        $table = New-SubEthaHashMap -Path (Join-Path $script:dir 'once') -Capacity 64 -KeySize 4 -ValueSize 8
        $first = $table.Observe()
        { $table.Observe() } | Should -Throw "*already observed by registration $($first.Id)*"
        $first.Close()
        $second = $table.Observe()
        $second.Closed() | Should -BeFalse -Because 'a closed registration frees the object for another'
        $second.Close()
    }

    It 'is freed once a registration nobody holds is collected' {
        $table = New-SubEthaHashMap -Path (Join-Path $script:dir 'collected') -Capacity 64 -KeySize 4 -ValueSize 8
        $null = $table.Observe()
        [GC]::Collect()
        [GC]::WaitForPendingFinalizers()
        $table.Observe().Close()
    }

    It 'is refused past the cap' {
        $table = New-SubEthaHashMap -Path (Join-Path $script:dir 'cap') -Capacity 64 -KeySize 4 -ValueSize 8
        $cap = (Get-SubEthaSidecar).MaxInstances
        Set-SubEthaSidecar -MaxInstances (Get-SubEthaSidecar).InstanceCount
        try {
            { $table.Observe() } | Should -Throw '*Set-SubEthaSidecar -MaxInstances*'
        } finally {
            Set-SubEthaSidecar -MaxInstances $cap
        }
        (Get-SubEthaSidecar).MaxInstances | Should -Be $cap
        $table.Observe().Close()
    }

    It 'is scanned by at least one thread' {
        $status = Get-SubEthaSidecar
        $status | Should -BeOfType [SubEtha.SidecarStatus]
        $status.NodeCount | Should -BeGreaterOrEqual 1
    }

    It 'has its sidecar status written by Set and Invoke only with -PassThru' {
        $cap = (Get-SubEthaSidecar).MaxInstances
        @(Set-SubEthaSidecar -MaxInstances $cap).Count | Should -Be 0
        $set = Set-SubEthaSidecar -MaxInstances $cap -PassThru
        $set | Should -BeOfType [SubEtha.SidecarStatus]
        $set.MaxInstances | Should -Be $cap
        @(Invoke-SubEthaSidecarScan).Count | Should -Be 0
        Invoke-SubEthaSidecarScan -PassThru | Should -BeOfType [SubEtha.SidecarStatus]
    }

    It 'answers to the SE aliases' {
        $obj = New-SEAdaptive
        $registration = $obj.Observe()
        try {
            $null = $obj.Record(1)
            Invoke-SESidecarScan
            $registration.Stats().OpsObserved | Should -Be 1
        } finally {
            $registration.Close()
        }
        $cap = (Get-SESidecar).MaxInstances
        Set-SESidecar -MaxInstances $cap
        (Get-SESidecar).MaxInstances | Should -Be $cap
    }
}

Describe "An adaptive object of the script's own" {
    It 'takes records only once it is observed' {
        $obj = New-SubEthaAdaptive
        $obj.Record(1) | Should -BeFalse -Because 'nothing drains an object nobody observes'
        $registration = $obj.Observe()
        try {
            $obj.Record(1) | Should -BeTrue
        } finally {
            $registration.Close()
        }
    }

    It 'moves its tag when the script sets it' {
        $obj = New-SubEthaAdaptive
        $obj.Tag() | Should -Be 0
        $obj.SetTag(7)
        $obj.Tag() | Should -Be 7
    }

    It "runs at the tag a ScriptBlock policy answers" {
        $asked = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
        [AppDomain]::CurrentDomain.SetData('subetha-sidecar-tests-asked', $asked)
        $obj = New-SubEthaAdaptive
        $registration = $obj.Observe({
                param($stats, $tag)
                $null = [AppDomain]::CurrentDomain.GetData('subetha-sidecar-tests-asked').Add("$($stats.OpsObserved),$tag")
                if ($stats.OpsObserved -ge 3) { 1 }
            })
        try {
            foreach ($i in 1..3) { $obj.Record(1, 100) | Should -BeTrue }
            Invoke-SubEthaSidecarScan
            $obj.Tag() | Should -Be 1 -Because "the policy's answer is the object's tag"
            $registration.Tag() | Should -Be 1
            $stats = $registration.Stats()
            $stats.MigrationsTriggered | Should -Be 1
            $stats.TotalLatencyTicks | Should -Be 300
            $stats.AverageLatencyTicks() | Should -Be 100
            $asked[$asked.Count - 1] | Should -Be '3,0' -Because 'the policy saw every record, at the tag before its answer'
            $registration.PolicyErrors() | Should -Be 0
            $registration.LastPolicyError() | Should -BeNullOrEmpty
        } finally {
            $registration.Close()
            [AppDomain]::CurrentDomain.SetData('subetha-sidecar-tests-asked', $null)
        }
    }

    It 'asks its policy on the scan thread, the same thread every scan' {
        $threads = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
        [AppDomain]::CurrentDomain.SetData('subetha-sidecar-tests-threads', $threads)
        $obj = New-SubEthaAdaptive
        $registration = $obj.Observe({
                param($stats, $tag)
                $null = [AppDomain]::CurrentDomain.GetData('subetha-sidecar-tests-threads').Add([AppDomain]::GetCurrentThreadId())
            })
        try {
            foreach ($i in 1..6) {
                $null = $obj.Record(1)
                Invoke-SubEthaSidecarScan
            }
            $threads.Count | Should -BeGreaterOrEqual 6
            @($threads | Sort-Object -Unique).Count |
                Should -Be 1 -Because 'a policy runs on the scan thread rather than on a thread started for each ask'
            $threads[0] | Should -Not -Be ([AppDomain]::GetCurrentThreadId()) -Because "the scan thread is not the caller's"
        } finally {
            $registration.Close()
            [AppDomain]::CurrentDomain.SetData('subetha-sidecar-tests-threads', $null)
        }
    }

    It 'counts a policy that throws and keeps its error record' {
        $obj = New-SubEthaAdaptive
        $registration = $obj.Observe({ param($stats, $tag) throw [System.DivideByZeroException]::new('no tag today') })
        try {
            $null = $obj.Record(1)
            Invoke-SubEthaSidecarScan
            $registration.PolicyErrors() | Should -Be 1
            $failure = $registration.LastPolicyError()
            $failure | Should -BeOfType [System.Management.Automation.ErrorRecord]
            $failure.Exception | Should -BeOfType [System.DivideByZeroException]
            $failure.Exception.Message | Should -Be 'no tag today'
            $failure.ScriptStackTrace | Should -Not -BeNullOrEmpty -Because 'the error record comes back whole'
            $obj.Tag() | Should -Be 0 -Because 'a failed policy leaves the tag where it was'
        } finally {
            $registration.Close()
        }
    }

    It 'keeps an answer of <answer> as an error rather than a tag' -ForEach @(
        @{ answer = "'fast'"; expected = "*answered 'fast'*" }
        @{ answer = '-1'; expected = "*answered '-1'*" }
        @{ answer = '4294967296'; expected = "*answered '4294967296'*" }
        @{ answer = '1.5'; expected = "*answered '1.5'*" }
        @{ answer = '1, 2'; expected = '*answered 2 objects*' }
    ) {
        $obj = New-SubEthaAdaptive
        $registration = $obj.Observe([scriptblock]::Create("param(`$stats, `$tag) $answer"))
        try {
            $null = $obj.Record(1)
            Invoke-SubEthaSidecarScan
            $registration.PolicyErrors() | Should -Be 1
            $failure = $registration.LastPolicyError()
            $failure.Exception | Should -BeOfType [System.Management.Automation.PSInvalidCastException]
            $failure.Exception.Message | Should -BeLike $expected
            $obj.Tag() | Should -Be 0
        } finally {
            $registration.Close()
        }
    }

    It 'takes only a ScriptBlock as a policy' {
        $obj = New-SubEthaAdaptive
        { $obj.Observe(42) } | Should -Throw '*a policy is a ScriptBlock*'
        $obj.Observe().Close()
    }

    It 'tells one recording thread from several' {
        $obj = New-SubEthaAdaptive
        $clock = [System.Diagnostics.Stopwatch]::StartNew()
        $registration = $obj.Observe()
        try {
            $null = $obj.Record(2, $null, $true)
            $null = Invoke-SEInRunspaces -Count 1 -Argument $obj -Script 'param($obj, $barrier) $null = $obj.Record(2)'
            $null = $obj.Record(3, $null, $null, $true)
            Invoke-SubEthaSidecarScan
            $stats = $registration.Stats()
        } finally {
            $registration.Close()
        }
        $elapsedUs = $clock.Elapsed.Ticks / 10
        $stats.DistinctThreadsFor(2) | Should -Be 2
        $stats.IsMultiThreadFor(2) | Should -BeTrue
        $stats.IsMultiThreadFor(3) | Should -BeFalse
        $stats.PerOpKindDistinctCount()[2] | Should -Be 2
        $threads = $stats.PerOpKindDistinctThreads()
        $threads.Count | Should -Be $stats.NOpKinds()
        $threads[2].Count | Should -Be $stats.MaxTrackedThreadsPerKind()
        @($threads[2] | Where-Object { $_ -ne 0 } | Sort-Object -Unique).Count |
            Should -Be 2 -Because 'two distinct thread ids, the rest empty'
        $stats.ContentionOps | Should -Be 1 -Because 'contended sets the flag ContentionRate counts'
        [Math]::Abs($stats.ContentionRate() - 1 / 3) | Should -BeLessThan 1e-9
        $stats.LastDrainUs | Should -BeLessOrEqual $elapsedUs -Because 'the last drain happened inside the test'
    }

    It 'has every record from every runspace counted once' {
        # Each runspace also asks for a scan, so the sidecar's own thread and
        # several callers of Invoke-SubEthaSidecarScan reach the ring together
        # while a ScriptBlock policy is asked on the sidecar's thread.
        $obj = New-SubEthaAdaptive
        $registration = $obj.Observe({ param($stats, $tag) })
        try {
            $accepted = Invoke-SEInRunspaces -Count 4 -Argument $obj -Script @'
param($obj, $barrier)
$barrier.SignalAndWait()
$taken = 0
foreach ($i in 1..500) { if ($obj.Record(1)) { $taken++ } }
Invoke-SubEthaSidecarScan
$taken
'@
            Invoke-SubEthaSidecarScan
            $registration.Stats().OpsObserved | Should -Be ($accepted | Measure-Object -Sum).Sum -Because (
                'every record the ring accepted is counted once, however many runspaces recorded and scanned')
            $registration.PolicyErrors() | Should -Be 0
        } finally {
            $registration.Close()
        }
    }

    It 'goes to one runspace of many asking for its registration' {
        $obj = New-SubEthaAdaptive
        $outcomes = Invoke-SEInRunspaces -Count 8 -Argument $obj -Script @'
param($obj, $barrier)
$barrier.SignalAndWait()
try { $obj.Observe() } catch { 'refused' }
'@
        $won = @($outcomes | Where-Object { $_ -isnot [string] })
        $won.Count | Should -Be 1 -Because 'an object has one registration however many runspaces ask'
        @($outcomes | Where-Object { $_ -eq 'refused' }).Count | Should -Be 7
        $won[0].Close()
    }
}

Describe 'The process' {
    It 'exits cleanly while a policy is being asked' {
        # The sidecar's thread asks the policy after each scan that drained
        # something; the records below leave it asking as the process exits.
        $child = @'
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
Import-Module (Join-Path $env:PWRS_MODULE 'SubEtha.psd1')
$obj = New-SubEthaAdaptive
$registration = $obj.Observe({ param($stats, $tag) Start-Sleep -Milliseconds 2 })
foreach ($i in 1..4000) { $null = $obj.Record(1) }
'@
        $encoded = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes($child))
        $info = New-Object System.Diagnostics.ProcessStartInfo (Get-Process -Id $PID).Path
        $info.Arguments = "-NoProfile -NonInteractive -EncodedCommand $encoded"
        $info.RedirectStandardOutput = $true
        $info.RedirectStandardError = $true
        $info.UseShellExecute = $false
        $process = [System.Diagnostics.Process]::Start($info)
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        $exited = $process.WaitForExit(120000)
        if (-not $exited) { $process.Kill() }
        $exited | Should -BeTrue -Because 'the process hung on its way out'
        $process.ExitCode | Should -Be 0 -Because "$($stdout.Result)$($stderr.Result)"
        $stderr.Result | Should -BeNullOrEmpty
    }
}
