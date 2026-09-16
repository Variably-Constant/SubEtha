# Several runspaces at once: on one object, on separate handles to one
# file, and waiting on each other.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'threads'

    # Starts $Script in another runspace with the arguments, and answers
    # a handle whose Wait-SEJob gives back what the script wrote.
    function Start-SEJob {
        param([scriptblock] $Script, [object[]] $Arguments)
        $ps = [powershell]::Create()
        $null = $ps.AddScript($Script.ToString())
        foreach ($a in $Arguments) { $null = $ps.AddArgument($a) }
        $handle = $ps.BeginInvoke()
        [pscustomobject] @{ Shell = $ps; Handle = $handle }
    }

    function Wait-SEJob {
        param($Job)
        $out = $Job.Shell.EndInvoke($Job.Handle)
        if ($Job.Shell.HadErrors) { throw ($Job.Shell.Streams.Error | Out-String) }
        $Job.Shell.Dispose()
        $out
    }
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'runspaces' {
    It 'add to one shared object from eight at once' {
        $a = New-SubEthaAtomic -Path (Join-Path $script:dir 'shared') -Init 0
        $jobs = 1..8 | ForEach-Object {
            Start-SEJob -Arguments @($a) -Script { param($atomic) for ($i = 0; $i -lt 1000; $i++) { $null = $atomic.FetchAdd(1) }; 'done' }
        }
        $jobs | ForEach-Object { Wait-SEJob $_ | Should -Be 'done' }
        $a.Load() | Should -Be 8000
        $a.Dispose()
    }

    It 'add through eight handles of their own to one file' {
        $p = Join-Path $script:dir 'handles'
        $a = New-SubEthaAtomic -Path $p -Init 0
        $manifest = Join-Path $env:PWRS_MODULE 'SubEtha.psd1'
        $jobs = 1..8 | ForEach-Object {
            Start-SEJob -Arguments @($manifest, $p) -Script {
                param($manifest, $path)
                Import-Module $manifest
                $mine = Open-SubEthaAtomic -Path $path
                for ($i = 0; $i -lt 1000; $i++) { $null = $mine.FetchAdd(1) }
                $mine.Dispose()
                'done'
            }
        }
        $jobs | ForEach-Object { Wait-SEJob $_ | Should -Be 'done' }
        $a.Load() | Should -Be 8000
        $a.Dispose()
    }

    It 'hand items from a producer to a waiting consumer' {
        # Each side holds its own handle to the channel. A call that
        # waits holds the object it was made on for as long as it
        # waits, so the send that would end a wait cannot go through
        # the same object.
        $p = Join-Path $script:dir 'channel'
        $ch = New-SubEthaChannel -Path $p -Capacity 64
        $manifest = Join-Path $env:PWRS_MODULE 'SubEtha.psd1'
        $consumer = Start-SEJob -Arguments @($manifest, $p) -Script {
            param($manifest, $path)
            Import-Module $manifest
            $channel = Open-SubEthaChannel -Path $path -Capacity 64
            $got = @()
            $deadline = [DateTime]::UtcNow.AddSeconds(10)
            while ($got.Count -lt 200 -and [DateTime]::UtcNow -lt $deadline) {
                $item = $channel.RecvFor(1)
                if ($null -ne $item) { $got += [System.Text.Encoding]::UTF8.GetString($item).TrimEnd([char]0) }
            }
            $channel.Dispose()
            $got.Count
        }
        for ($i = 0; $i -lt 200; $i++) { $ch.SendFor("item-$i", 5) | Should -BeTrue }
        Wait-SEJob $consumer | Should -Be 200
        $ch.Dispose()
    }

    It 'block a second runspace on a write hold until it is released' {
        $lock = New-SubEthaRWLock -Path (Join-Path $script:dir 'lock')
        $hold = $lock.Write()
        $waiter = Start-SEJob -Arguments @($lock) -Script {
            param($l)
            $first = $null -eq $l.TryWrite()
            $h = $l.WriteFor(5)
            $second = $null -ne $h
            if ($h) { $h.Release() }
            "$first $second"
        }
        Start-Sleep -Milliseconds 200
        $hold.Release()
        Wait-SEJob $waiter | Should -Be 'True True'
        $lock.Dispose()
    }

    It 'share two permits among four runspaces' {
        $sem = New-SubEthaSemaphore -Path (Join-Path $script:dir 'sem') -Initial 2
        $jobs = 1..4 | ForEach-Object {
            Start-SEJob -Arguments @($sem) -Script {
                param($s)
                $permit = $s.AcquireFor(10)
                if ($null -eq $permit) { 'timed out' } else { Start-Sleep -Milliseconds 100; $permit.Release(); 'held' }
            }
        }
        $jobs | ForEach-Object { Wait-SEJob $_ | Should -Be 'held' }
        $sem.Available() | Should -Be 2
        $sem.Dispose()
    }

    It 'drain two producers sending from two runspaces' {
        $ring = New-SubEthaRing -Path (Join-Path $script:dir 'ring') -Capacity 256 -MaxProducers 2
        $c = $ring.RegisterConsumer()
        $producers = 1..2 | ForEach-Object {
            Start-SEJob -Arguments @($ring, $_) -Script {
                param($r, $n)
                $p = $r.RegisterProducer()
                $sent = 0
                $deadline = [DateTime]::UtcNow.AddSeconds(10)
                while ($sent -lt 500 -and [DateTime]::UtcNow -lt $deadline) {
                    if ($r.Send($p, "p$n-$sent")) { $sent++ } else { Start-Sleep -Milliseconds 1 }
                }
                $sent
            }
        }
        $received = 0
        $deadline = [DateTime]::UtcNow.AddSeconds(15)
        while ($received -lt 1000 -and [DateTime]::UtcNow -lt $deadline) {
            $batch = $ring.RecvMany($c, 64)
            if ($batch.Count -eq 0) { Start-Sleep -Milliseconds 1 } else { $received += $batch.Count }
        }
        $producers | ForEach-Object { Wait-SEJob $_ | Should -Be 500 }
        $received | Should -Be 1000
        $ring.Dispose()
    }

    It 'keep a pinned view steady while another runspace removes and sweeps' {
        # The map keeps one entry per key, so a live pin is held open
        # against a remove, whose tombstone the pin still reaches, not
        # against a re-insert, which would leave the old value nowhere
        # to live. A sweep run while the pin is held takes nothing.
        $m = New-SubEthaVersionedMap -Path (Join-Path $script:dir 'vmap') -Capacity 64 -EpochsPath (Join-Path $script:dir 'vmap-epochs')
        $null = $m.Insert(1, 11)
        $reader = Start-SEJob -Arguments @($m) -Script {
            param($map)
            $pin = $map.Pin()
            $before = $pin.Get(1)
            Start-Sleep -Milliseconds 300
            $after = $pin.Get(1)
            $pin.Release()
            "$before $after"
        }
        Start-Sleep -Milliseconds 100
        $m.Remove(1) | Should -Be 11
        $m.Sweep() | Should -Be 0
        Wait-SEJob $reader | Should -Be '11 11'
        $null -eq $m.Get(1) | Should -BeTrue
        $m.Dispose()
    }
}
