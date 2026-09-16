# A second process of the same host attaches to the files this one
# made, which is what the whole library is for.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'processes'
    $script:exe = [System.Diagnostics.Process]::GetCurrentProcess().MainModule.FileName
    $script:manifest = Join-Path $env:PWRS_MODULE 'SubEtha.psd1'

    # Runs $Body in a fresh process of the host running this suite, with
    # the module imported first, and answers its output lines as one
    # array, so a single line still indexes as a line and not as its
    # characters. A child that fails, fails the test with its own output.
    function Invoke-SEChild {
        param([string] $Body)
        $file = Join-Path $script:dir ('child-' + [guid]::NewGuid().ToString('n') + '.ps1')
        $text = "`$ErrorActionPreference = 'Stop'`r`nImport-Module '$($script:manifest)'`r`n$Body"
        [System.IO.File]::WriteAllText($file, $text)
        $out = @(& $script:exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $file 2>&1 | ForEach-Object { "$_" })
        if ($LASTEXITCODE -ne 0) { throw "the child process failed: $($out -join [Environment]::NewLine)" }
        Write-Output -NoEnumerate $out
    }
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'two processes on one file' {
    It 'share a counter' {
        $p = Join-Path $script:dir 'hits'
        $a = New-SubEthaAtomic -Path $p -Init 10
        $out = Invoke-SEChild "`$c = Open-SubEthaAtomic -Path '$p'; `$null = `$c.FetchAdd(5); Write-Output `$c.Load(); `$c.Dispose()"
        [int] $out[-1] | Should -Be 15
        $a.Load() | Should -Be 15
        $a.Dispose()
    }

    It 'send items through a ring the other made' {
        $p = Join-Path $script:dir 'ring'
        $ring = New-SubEthaRing -Path $p -Capacity 16 -MaxProducers 2 -MaxConsumers 2
        $c = $ring.RegisterConsumer()
        $out = Invoke-SEChild "`$r = Open-SubEthaRing -Path '$p' -Capacity 16 -MaxProducers 2 -MaxConsumers 2; `$prod = `$r.RegisterProducer(); Write-Output `$r.SendMany(`$prod, @('one', 'two', 'three')); `$r.Dispose()"
        [int] $out[-1] | Should -Be 3
        ($ring.RecvMany($c, 10) | ForEach-Object { ConvertFrom-SEBytes $_ }) -join ',' | Should -Be 'one,two,three'
        $ring.Dispose()
    }

    It 'pass a channel item and read it back with a wait' {
        $p = Join-Path $script:dir 'channel'
        $ch = New-SubEthaChannel -Path $p -Capacity 8
        $null = Invoke-SEChild "`$c = Open-SubEthaChannel -Path '$p' -Capacity 8; if (-not `$c.Send('from the child')) { throw 'full' }; `$c.Dispose()"
        Get-SEText $ch.RecvFor(2) | Should -Be 'from the child'
        $ch.Dispose()
    }

    It 'see a write lock the other holds' {
        $p = Join-Path $script:dir 'lock'
        $lock = New-SubEthaRWLock -Path $p
        $hold = $lock.Write()
        $busy = Invoke-SEChild "`$l = Open-SubEthaRWLock -Path '$p'; `$h = `$l.TryWrite(); Write-Output (`$null -eq `$h); if (`$h) { `$h.Release() }; `$l.Dispose()"
        $busy[-1] | Should -Be 'True'
        $hold.Release()
        $free = Invoke-SEChild "`$l = Open-SubEthaRWLock -Path '$p'; `$h = `$l.WriteFor(2); Write-Output (`$null -ne `$h); if (`$h) { `$h.Release() }; `$l.Dispose()"
        $free[-1] | Should -Be 'True'
        $lock.Dispose()
    }

    It 'see a permit the other holds' {
        $p = Join-Path $script:dir 'sem'
        $sem = New-SubEthaSemaphore -Path $p -Initial 1
        $permit = $sem.Acquire()
        $none = Invoke-SEChild "`$s = Open-SubEthaSemaphore -Path '$p' -MaxPermits 1; Write-Output (`$null -eq `$s.TryAcquire()); `$s.Dispose()"
        $none[-1] | Should -Be 'True'
        $permit.Release()
        $got = Invoke-SEChild "`$s = Open-SubEthaSemaphore -Path '$p' -MaxPermits 1; `$h = `$s.AcquireFor(2); Write-Output (`$null -ne `$h); `$h.Release(); `$s.Dispose()"
        $got[-1] | Should -Be 'True'
        $sem.Available() | Should -Be 1
        $sem.Dispose()
    }

    It 'record who holds a lease by process id' {
        $p = Join-Path $script:dir 'lease'
        $lease = New-SubEthaOwnerLease -Path $p -Value 'job'
        $out = Invoke-SEChild "`$l = Open-SubEthaOwnerLease -Path '$p'; if (-not `$l.TryAcquire()) { throw 'refused' }; `$null = `$l.Write('mine'); Write-Output `$PID; `$l.Dispose()"
        $childPid = [int] $out[-1]
        $lease.Owner() | Should -Be $childPid
        $lease.HeldBy() | Should -BeFalse
        Get-SEText $lease.Read($childPid) | Should -Be 'mine'
        $lease.Release($childPid) | Should -BeTrue
        $null -eq $lease.Owner() | Should -BeTrue
        $lease.Dispose()
    }

    It 'publish and subscribe across the boundary' {
        $p = Join-Path $script:dir 'pubsub'
        $pub = New-SubEthaPubSub -Path $p -Capacity 8
        $sub = $pub.Subscribe()
        $null = Invoke-SEChild "`$q = Open-SubEthaPubSub -Path '$p' -Capacity 8; `$null = `$q.PublishMany(@('a', 'b')); `$q.Dispose()"
        ($sub.NextMany(10) | ForEach-Object { Get-SEText $_ }) -join '' | Should -Be 'ab'
        $sub.Dispose()
        $pub.Dispose()
    }
}
