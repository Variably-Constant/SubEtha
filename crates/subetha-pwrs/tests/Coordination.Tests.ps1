# The structures processes agree through.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'coordination'
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'SubEtha.NotifierSet' {
    It 'signals every attached notifier' {
        $set = New-SubEthaNotifierSet -Path (Join-Path $script:dir 'notify')
        $a = $set.Attach()
        $b = $set.Attach()
        $set.Attached() | Should -Be 2
        $a.IsSignaled() | Should -BeFalse
        $a.Wait(0.05) | Should -BeFalse
        $set.Signal() | Should -Be 2
        $a.Wait(1) | Should -BeTrue
        $b.Wait(1) | Should -BeTrue
        $a.Drain()
        $a.Wait(0.05) | Should -BeFalse
        $a.Index | Should -Not -Be $b.Index
        $a.Dispose()
        $b.Dispose()
        $set.Dispose()
    }
}

Describe 'SubEtha.LeaderElection' {
    It 'gives the role to one process and takes it back' {
        $e = New-SubEthaLeaderElection -Path (Join-Path $script:dir 'election')
        $null -eq $e.Leader() | Should -BeTrue
        $e.TryClaim() | Should -BeTrue
        $e.IsLeader() | Should -BeTrue
        $e.Leader() | Should -Be $PID
        $e.Beat() | Should -BeTrue
        $t = $e.Term()
        $e.StepDown() | Should -BeTrue
        $null -eq $e.Leader() | Should -BeTrue
        $e.TryClaim(3, 4242) | Should -BeTrue
        $e.Term() | Should -BeGreaterThan $t
        $e.TickEpoch() | Should -BeGreaterThan 0
        $e.GlobalEpoch() | Should -BeGreaterThan 0
        $e.Dispose()
    }
}

Describe 'SubEtha.HolderTable' {
    It 'claims, publishes and releases slots' {
        $h = New-SubEthaHolderTable -Path (Join-Path $script:dir 'holders') -Capacity 2
        $h.Capacity | Should -Be 2
        $s = $h.Claim(42)
        $s | Should -Not -BeNullOrEmpty
        $h.Payload($s) | Should -Be 42
        $r = $h.Reserve()
        $h.Live() | Should -Be 2
        $null -eq $h.Claim(1) | Should -BeTrue
        $h.Publish($r, 7)
        $h.Payload($r) | Should -Be 7
        $h.Release($s)
        $null -eq $h.Payload($s) | Should -BeTrue
        $h.Live() | Should -Be 1
        $h.Dispose()
    }
}

Describe 'SubEtha.Heartbeat and SubEtha.EpochBarrier' {
    It 'registers, beats and snapshots a slot' {
        $hb = New-SubEthaHeartbeat -Path (Join-Path $script:dir 'heartbeat') -Capacity 4
        $slot = $hb.Register()
        $hb.Beat($slot)
        $snap = $hb.Snapshot($slot)
        $snap.Pid | Should -Be $PID
        $null -eq $hb.Snapshot(3) | Should -BeTrue
        $hb.TickGlobalEpoch() | Should -BeGreaterThan 0
        $hb.GlobalEpoch() | Should -BeGreaterThan 0
        $hb.Unregister($slot)
        $hb.Dispose()
    }

    It 'opens a barrier that one live peer passes alone' {
        $hbPath = Join-Path $script:dir 'barrier-hb'
        $hb = New-SubEthaHeartbeat -Path $hbPath -Capacity 4
        $slot = $hb.Register()
        $hb.Beat($slot)
        $barrier = $hb.Barrier((Join-Path $script:dir 'barrier'))
        $barrier.GetType().FullName | Should -Be 'SubEtha.EpochBarrier'
        $barrier.GraceEpochs | Should -Be 3
        $barrier.LivePeers() | Should -Be 1
        $epoch = $barrier.CurrentEpoch()
        $barrier.Wait($epoch, 2) | Should -BeTrue
        $barrier.CurrentEpoch() | Should -BeGreaterThan $epoch
        $barrier.Wait($barrier.CurrentEpoch(), 0.1, 2) | Should -BeFalse
        $barrier.Arrived() | Should -BeGreaterOrEqual 0
        $again = Open-SubEthaEpochBarrier -Path (Join-Path $script:dir 'barrier') -HeartbeatPath $hbPath -Capacity 4
        $again.LivePeers() | Should -Be 1
        $again.Dispose()
        $barrier.Dispose()
        $hb.Dispose()
    }
}

Describe 'SubEtha.Condvar' {
    It 'wakes a waiter whose condition a script block answers' {
        $path = Join-Path $script:dir 'condvar'
        $cv = New-SubEthaCondvar -Path $path
        $g = $cv.Generation()
        $script:flag = $false
        Wait-SubEthaCondition -Path $path -Until { $true } | Should -BeTrue
        Wait-SubEthaCondition -Path $path -Until { $script:flag } -Timeout 0.2 | Should -BeFalse
        $cv.NotifyOne() | Should -BeGreaterOrEqual 0
        $cv.NotifyAll() | Should -BeGreaterOrEqual 0
        $cv.Generation() | Should -BeGreaterThan $g
        { Wait-SubEthaCondition -Path $path -Until { throw 'inside' } -Timeout 0.2 -ErrorAction Stop } | Should -Throw
        $cv.Dispose()
    }
}

Describe 'SubEtha.FenceClock' {
    It 'ticks, merges and fences' {
        $c = New-SubEthaFenceClock -Path (Join-Path $script:dir 'fence') -Capacity 4
        $c.Capacity | Should -Be 4
        $slot = $c.Register()
        $t1 = $c.Tick($slot)
        $t1.GetType().FullName | Should -Be 'SubEtha.ClockReading'
        $t2 = $c.Tick($slot)
        ($t2.PhysicalUs -gt $t1.PhysicalUs) -or ($t2.Logical -gt $t1.Logical) | Should -BeTrue
        $merged = $c.Merge($slot, $t2.PhysicalUs + 1000000, 3)
        $merged.PhysicalUs | Should -BeGreaterOrEqual ($t2.PhysicalUs + 1000000)
        $local = $c.GetLocal($slot)
        $local.PhysicalUs | Should -Be $merged.PhysicalUs
        $fence = $c.GlobalFence()
        $fence.PhysicalUs | Should -BeGreaterOrEqual $local.PhysicalUs
        $c.SharedClockUs() | Should -BeGreaterThan 0
        $c.Unregister($slot)
        $c.Dispose()
    }
}

Describe 'SubEtha.Epochs' {
    It 'advances, claims tickets and publishes them' {
        $e = New-SubEthaEpochs -Path (Join-Path $script:dir 'epochs') -Capacity 4
        $e.Capacity | Should -Be 4
        $before = $e.Now()
        $e.Advance() | Should -BeGreaterThan $before
        $t = $e.ClaimTicket()
        $t.GetType().FullName | Should -Be 'SubEtha.EpochTicket'
        $e.OpenTickets() | Should -Be 1
        $e.Now() | Should -Be ($t.Epoch - 1)
        $e.PublishTicket($t.Slot)
        $e.OpenTickets() | Should -Be 0
        $e.Now() | Should -BeGreaterOrEqual $t.Epoch
        @($e.DeadTickets()).Count | Should -Be 0
        $e.FreeDeadTicket(1) | Should -BeFalse
        $e.Dispose()
    }
}
