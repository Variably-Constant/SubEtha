# The holds: the reader-writer lock, the semaphore and the owner lease.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'locks'
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'SubEtha.RWLock' {
    It 'hands out read and write holds that release' {
        $lock = New-SubEthaRWLock -Path (Join-Path $script:dir 'rw')
        $lock.Readers() | Should -Be 0
        $r = $lock.Read()
        $r.GetType().FullName | Should -Be 'SubEtha.Hold'
        $r.Write | Should -BeFalse
        $r.Held | Should -BeTrue
        $lock.Readers() | Should -Be 1
        $null -eq $lock.TryWrite() | Should -BeTrue
        $null -eq $lock.WriteFor(0.1) | Should -BeTrue
        $r2 = $lock.TryRead()
        $r2.Held | Should -BeTrue
        $lock.Readers() | Should -Be 2
        $r.Release()
        $r.Held | Should -BeFalse
        $r.Release()
        $r2.Dispose()
        $lock.Readers() | Should -Be 0
        $w = $lock.Write()
        $w.Write | Should -BeTrue
        $null -eq $lock.TryRead() | Should -BeTrue
        $null -eq $lock.ReadFor(0.1) | Should -BeTrue
        $w.Release()
        $rf = $lock.ReadFor(1)
        $rf.Held | Should -BeTrue
        $rf.Release()
        $wf = $lock.WriteFor(1)
        $wf.Held | Should -BeTrue
        $wf.Release()
        $lock.Dispose()
    }

    It 'is shared through a second handle' {
        $path = Join-Path $script:dir 'rw-shared'
        $a = New-SubEthaRWLock -Path $path
        $b = Open-SubEthaRWLock -Path $path
        $w = $a.Write()
        $null -eq $b.TryWrite() | Should -BeTrue
        $w.Release()
        $bw = $b.TryWrite()
        $bw.Held | Should -BeTrue
        $bw.Release()
        $a.Dispose()
        $b.Dispose()
    }
}

Describe 'SubEtha.Semaphore' {
    It 'limits permits and gives them back' {
        $s = New-SubEthaSemaphore -Path (Join-Path $script:dir 'sem') -Initial 2
        $s.MaxPermits | Should -Be 2
        $s.Available() | Should -Be 2
        $p1 = $s.Acquire()
        $p1.GetType().FullName | Should -Be 'SubEtha.PermitHold'
        $p2 = $s.TryAcquire()
        $p2.Held | Should -BeTrue
        $s.Available() | Should -Be 0
        $null -eq $s.TryAcquire() | Should -BeTrue
        $null -eq $s.AcquireFor(0.1) | Should -BeTrue
        $p1.Release()
        $p1.Held | Should -BeFalse
        $s.Available() | Should -Be 1
        $p3 = $s.AcquireFor(1)
        $p3.Held | Should -BeTrue
        $p2.Dispose()
        $p3.Dispose()
        $s.Available() | Should -Be 2
        $s.Waiters() | Should -Be 0
        { New-SubEthaSemaphore -Path (Join-Path $script:dir 'sem-bad') -Initial 3 -MaxPermits 2 -ErrorAction Stop } | Should -Throw
        $s.Dispose()
    }

    It 'is shared through a second handle' {
        $path = Join-Path $script:dir 'sem-shared'
        $a = New-SubEthaSemaphore -Path $path -Initial 1
        $b = Open-SubEthaSemaphore -Path $path -MaxPermits 1
        $p = $a.Acquire()
        $null -eq $b.TryAcquire() | Should -BeTrue
        $p.Release()
        $q = $b.TryAcquire()
        $q.Held | Should -BeTrue
        $q.Release()
        $a.Dispose()
        $b.Dispose()
    }
}

Describe 'SubEtha.OwnerLease' {
    It 'is taken, beaten, read, written and released' {
        $l = New-SubEthaOwnerLease -Path (Join-Path $script:dir 'lease') -Value 'token'
        $l.MaxValueBytes | Should -Be 44
        $null -eq $l.Owner() | Should -BeTrue
        $l.TryAcquire() | Should -BeTrue
        $l.TryAcquire() | Should -BeTrue
        $l.Owner() | Should -Be $PID
        $l.HeldBy() | Should -BeTrue
        Get-SEText $l.Read() | Should -Be 'token'
        $l.Write('changed') | Should -BeTrue
        Get-SEText $l.Read() | Should -Be 'changed'
        $l.Beat() | Should -BeTrue
        $l.TickEpoch() | Should -BeGreaterThan 0
        $t = $l.Term()
        $l.Release() | Should -BeTrue
        $l.Release() | Should -BeFalse
        $null -eq $l.Read() | Should -BeTrue
        $l.Flush()
        $l.FlushAsync()
        $l.Dispose()
        $again = Open-SubEthaOwnerLease -Path (Join-Path $script:dir 'lease')
        $again.Term() | Should -Be $t
        $again.Dispose()
    }

    It 'lets a lower process id take over and refuses a higher one' {
        $l = New-SubEthaOwnerLease -Path (Join-Path $script:dir 'lease-pids')
        $l.TryAcquire(0, $PID) | Should -BeTrue
        $l.TryAcquire(0, $PID + 1) | Should -BeFalse
        $l.TryAcquire(0, 1) | Should -BeTrue
        $l.Owner() | Should -Be 1
        $l.HeldBy($PID) | Should -BeFalse
        $l.Release(1) | Should -BeTrue
        $l.Dispose()
    }

    It 'hands out a hold that gives the lease back' {
        $l = New-SubEthaOwnerLease -Path (Join-Path $script:dir 'lease-hold') -Value 'v'
        $h = $l.Hold()
        $h.GetType().FullName | Should -Be 'SubEtha.LeaseHold'
        $h.Pid | Should -Be $PID
        $h.Held | Should -BeTrue
        Get-SEText $h.Read() | Should -Be 'v'
        $h.Write('w') | Should -BeTrue
        $h.Beat() | Should -BeTrue
        $l.Owner() | Should -Be $PID
        $h.Release()
        $h.Held | Should -BeFalse
        $null -eq $l.Owner() | Should -BeTrue
        $l.TryAcquire(0, 1) | Should -BeTrue
        { $l.Hold(0, $PID + 1) } | Should -Throw
        $l.Release(1) | Should -BeTrue
        $l.Dispose()
    }

    It 'resets a wedged lease' {
        $path = Join-Path $script:dir 'lease-reset'
        $l = New-SubEthaOwnerLease -Path $path
        $l.TryAcquire(0, 1) | Should -BeTrue
        $l.Dispose()
        $fresh = New-SubEthaOwnerLease -Path $path -Value 'clean' -Reset
        $null -eq $fresh.Owner() | Should -BeTrue
        $fresh.TryAcquire() | Should -BeTrue
        Get-SEText $fresh.Read() | Should -Be 'clean'
        $fresh.Dispose()
    }
}
