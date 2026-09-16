# The front door: the channel, the adaptive queue, the work queue, the
# map and the policy.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'frontdoor'
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'SubEtha.Channel' {
    It 'sends, receives and waits' {
        $path = Join-Path $script:dir 'channel'
        $c = New-SubEthaChannel -Path $path -Capacity 8
        $c.MaxItemSize | Should -Be 52
        $null -eq $c.Recv() | Should -BeTrue
        $null -eq $c.RecvFor(0.1) | Should -BeTrue
        $c.Send('one') | Should -BeTrue
        $c.SendMany(@('two', 'three')) | Should -Be 2
        Get-SEText $c.Recv() | Should -Be 'one'
        ($c.RecvMany(10) | ForEach-Object { Get-SEText $_ }) -join ',' | Should -Be 'two,three'
        $c.SendFor('four', 1) | Should -BeTrue
        Get-SEText $c.RecvFor(1) | Should -Be 'four'
        { $c.Send([byte[]]::new(53)) } | Should -Throw
        $other = Open-SubEthaChannel -Path $path -Capacity 8
        $c.Send('five') | Should -BeTrue
        Get-SEText $other.Recv() | Should -Be 'five'
        $other.Dispose()
        $c.Dispose()
    }
}

Describe 'SubEtha.AdaptiveQueue' {
    It 'moves items and reports its shape and traffic' {
        $q = New-SubEthaAdaptiveQueue -Path (Join-Path $script:dir 'adaptive') -Capacity 16
        $q.Shape() | Should -Be ([SubEtha.QueueShape]::Ring)
        $q.Send('a') | Should -BeTrue
        $q.SendMany(@('b', 'c')) | Should -Be 2
        Get-SEText $q.Recv() | Should -Be 'a'
        ($q.RecvMany(10) | ForEach-Object { Get-SEText $_ }) -join '' | Should -Be 'bc'
        $null -eq $q.RecvFor(0.1) | Should -BeTrue
        $q.SendFor('d', 1) | Should -BeTrue
        Get-SEText $q.RecvFor(1) | Should -Be 'd'
        $t = $q.Traffic()
        $t.AverageBatch | Should -BeGreaterOrEqual 1
        $q.ShapeGeneration() | Should -BeGreaterOrEqual 0
        $q.Ordering() | Should -Be ([SubEtha.OrderingNeed]::PerProducer)
        $q.SetOrdering([SubEtha.OrderingNeed]::GlobalFifo)
        $q.Ordering() | Should -Be ([SubEtha.OrderingNeed]::GlobalFifo)
        $q.Inversions() | Should -BeGreaterOrEqual 0
        $moved = $q.MaybeChangeShape()
        ($null -eq $moved) -or ($moved -is [SubEtha.QueueShape]) | Should -BeTrue
        { $q.ChangeShapeTo([SubEtha.QueueShape]::Map) } | Should -Throw
        $q.Dispose()
    }
}

Describe 'SubEtha.WorkQueue' {
    It 'is popped by its owner and stolen by a thief' {
        $path = Join-Path $script:dir 'work'
        $owner = New-SubEthaWorkQueue -Path $path -Capacity 16 -Thieves 1
        $owner.Thief | Should -BeFalse
        $owner.Push('a') | Should -BeTrue
        $owner.PushMany(@('b', 'c')) | Should -Be 2
        $thief = Open-SubEthaWorkQueue -Path $path
        $thief.Thief | Should -BeTrue
        Get-SEText $thief.Steal() | Should -Be 'a'
        Get-SEText $owner.Pop() | Should -Be 'c'
        ($thief.StealMany(10) | ForEach-Object { Get-SEText $_ }) -join '' | Should -Be 'b'
        $null -eq $thief.Steal() | Should -BeTrue
        $thief.Dispose()
        $owner.Dispose()
    }
}

Describe 'SubEtha.KvMap' {
    It 'maps integers to integers' {
        $m = New-SubEthaKvMap -Path (Join-Path $script:dir 'kv') -Capacity 64
        $m.Insert(1, 10) | Should -BeTrue
        $m.Insert(1, 11) | Should -BeFalse
        $m.InsertMany(@([uint64] 2, [uint64] 3), @([uint64] 20, [uint64] 30)) | Should -Be @($true, $true)
        $m.Get(1) | Should -Be 11
        $null -eq $m.Get(9) | Should -BeTrue
        $many = $m.GetMany(@([uint64] 2, [uint64] 9))
        $many[0] | Should -Be 20
        $null -eq $many[1] | Should -BeTrue
        $m.Contains(3) | Should -BeTrue
        $m.Count() | Should -Be 3
        $m.Dispose()
    }
}

Describe 'SubEtha.QosPolicy' {
    It 'records what a stream needs and what follows from it' {
        $p = New-SubEthaQosPolicy
        $p.Durability() | Should -Be ([SubEtha.Durability]::Volatile)
        $p.Reliability() | Should -Be ([SubEtha.Reliability]::BestEffort)
        $p.KeepLast() | Should -Be 1024
        $p.MaxLatency() | Should -Be 0.1
        $p.Ordering() | Should -Be ([SubEtha.OrderingNeed]::PerProducer)
        $p.SetDurability([SubEtha.Durability]::Persistent)
        $p.SetReliability('Reliable')
        $p.SetKeepLast($null)
        $p.SetMaxLatency(2.5)
        $p.SetOrdering([SubEtha.OrderingNeed]::GlobalFifo)
        $s = $p.Snapshot()
        $s.GetType().FullName | Should -Be 'SubEtha.QosSnapshot'
        $s.Durability | Should -Be ([SubEtha.Durability]::Persistent)
        $s.Reliability | Should -Be ([SubEtha.Reliability]::Reliable)
        $null -eq $s.KeepLast | Should -BeTrue
        $s.MaxLatency | Should -Be 2.5
        $s.Ordering | Should -Be ([SubEtha.OrderingNeed]::GlobalFifo)
        $s.RecommendsLocaleChange([SubEtha.Locale]::Anon) | Should -Be ([SubEtha.Locale]::File)
        $null -eq $s.RecommendsLocaleChange([SubEtha.Locale]::File) | Should -BeTrue
        $s.RecommendsOrderingChange([SubEtha.OrderingNeed]::PerProducer) | Should -Be ([SubEtha.OrderingNeed]::GlobalFifo)
        $null -eq $s.RecommendsOrderingChange([SubEtha.OrderingNeed]::GlobalFifo) | Should -BeTrue
        $p.Dispose()
    }

    It 'starts from a preset and overrides it' {
        $p = New-SubEthaQosPolicy -Preset PersistentLog -MaxLatency 0.5
        $p.Durability() | Should -Be ([SubEtha.Durability]::Persistent)
        $p.Reliability() | Should -Be ([SubEtha.Reliability]::Reliable)
        $null -eq $p.KeepLast() | Should -BeTrue
        $p.MaxLatency() | Should -Be 0.5
        $p.Dispose()
        $s = New-SubEthaQosPolicy -Preset Streaming
        $s.Durability() | Should -Be ([SubEtha.Durability]::Volatile)
        $s.Dispose()
        { New-SubEthaQosPolicy -KeepAll -KeepLast 5 -ErrorAction Stop } | Should -Throw
    }
}
