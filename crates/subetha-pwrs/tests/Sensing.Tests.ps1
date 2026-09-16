# The link across a lossy network and the sensors.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
}

Describe 'SubEtha.SensSender and SubEtha.SensReceiver' {
    It 'carries items over loopback' {
        $recv = New-SubEthaSensReceiver -LocalPort 0 -MaxItemSize 64 -LocalHost 127.0.0.1 -Code Rlc
        $recv.MaxItemSize | Should -Be 64
        $addr = $recv.LocalAddr()
        $addr.GetType().FullName | Should -Be 'SubEtha.Endpoint'
        $addr.Port | Should -BeGreaterThan 0
        $recv.Alive() | Should -BeTrue
        $send = New-SubEthaSensSender -PeerHost 127.0.0.1 -PeerPort $addr.Port -MaxItemSize 64 -LocalHost 127.0.0.1 -Code Rlc
        $send.Peer.Port | Should -Be $addr.Port
        $send.LocalAddr().Port | Should -BeGreaterThan 0
        $send.Code() | Should -Be ([SubEtha.SensCode]::Rlc)
        $null -eq $send.Loss() | Should -BeTrue
        $send.Send('hello')
        $send.SendMany(@('one', 'two')) | Should -Be 2
        { $send.Send([byte[]]::new(65)) } | Should -Throw
        $got = @()
        $deadline = [DateTime]::UtcNow.AddSeconds(10)
        while ($got.Count -lt 3 -and [DateTime]::UtcNow -lt $deadline) {
            $got += $recv.Poll() | ForEach-Object { ConvertFrom-SEBytes $_ }
            if ($got.Count -lt 3) { Start-Sleep -Milliseconds 20 }
        }
        $got | Should -Be @('hello', 'one', 'two')
        $send.DatagramsSent() | Should -BeGreaterThan 0
        $send.DatagramsReceived() | Should -BeGreaterOrEqual 0
        $send.Switches() | Should -BeGreaterOrEqual 0
        $recv.Switches() | Should -BeGreaterOrEqual 0
        $recv.SendFailures() | Should -BeGreaterOrEqual 0
        $recv.Code() | Should -Be ([SubEtha.SensCode]::Rlc)
        $send.Send('tagged')
        $sourced = @()
        $deadline = [DateTime]::UtcNow.AddSeconds(10)
        while ($sourced.Count -lt 1 -and [DateTime]::UtcNow -lt $deadline) {
            $sourced += $recv.PollFrom()
            if ($sourced.Count -lt 1) { Start-Sleep -Milliseconds 20 }
        }
        $sourced[0].GetType().FullName | Should -Be 'SubEtha.SourcedItem'
        ConvertFrom-SEBytes $sourced[0].Bytes | Should -Be 'tagged'
        $send.Dispose()
        $recv.Dispose()
    }
}

Describe 'the sensors' {
    It 'classifies a loss from the spacing' {
        $k = New-SubEthaLossKind
        $k.GetType().FullName | Should -Be 'SubEtha.LossKind'
        1..20 | ForEach-Object { $k.ObserveSpacing(1000) }
        $k.ObserveDelay(500)
        $k.Classify(1, 1000) | Should -Be ([SubEtha.LossClass]::Wireless)
        $k.Classify(1, 5000) | Should -Be ([SubEtha.LossClass]::Congestion)
        $k.CongestionShare() | Should -BeGreaterOrEqual 0
        $k.DelaySpread() | Should -BeGreaterOrEqual 0
        { $k.ObserveSpacing(-1) } | Should -Throw
        $k.Dispose()
    }

    It 'measures runs of losses' {
        $b = New-SubEthaLossBursts
        $b.GetType().FullName | Should -Be 'SubEtha.LossBursts'
        $null -eq $b.MeanRunLength() | Should -BeTrue
        $pattern = @()
        1..50 | ForEach-Object { $pattern += @($true, $true, $true, $false, $false, $false, $false, $false, $false, $false) }
        $b.ObserveMany($pattern) | Should -Be 500
        $b.Observe($false)
        $b.Samples() | Should -Be 501
        $b.MeanRunLength() | Should -BeGreaterThan 1
        $b.SteadyLoss() | Should -BeGreaterThan 0
        $rates = $b.TransitionRates()
        $rates.GetType().FullName | Should -Be 'SubEtha.BurstRates'
        $rates.Entering | Should -BeGreaterThan 0
        $rates.Leaving | Should -BeGreaterThan 0
        $b.Dispose()
    }

    It 'reads jitter, spacing and trend' {
        $t = New-SubEthaTiming -Window 8
        $t.GetType().FullName | Should -Be 'SubEtha.Timing'
        $t.Window | Should -Be 8
        0..15 | ForEach-Object { $t.Observe(1000 * $_, 1000 * $_ + 50 + ($_ % 3) * 10) }
        $t.Samples() | Should -BeGreaterThan 0
        $t.Jitter() | Should -BeGreaterOrEqual 0
        $t.Spacing() | Should -BeGreaterThan 0
        $t.ClockSkew() | Should -Not -BeNullOrEmpty
        $t.Trend() | Should -Not -BeNullOrEmpty
        $t.TrendDebiased() | Should -Not -BeNullOrEmpty
        { New-SubEthaTiming -Window 1 -ErrorAction Stop } | Should -Throw
        $t.Dispose()
    }

    It 'sees two groups of round trips' {
        $s = New-SubEthaRoundTripShape
        $s.GetType().FullName | Should -Be 'SubEtha.RoundTripShape'
        $null -eq $s.TwoGroups() | Should -BeTrue
        $samples = @()
        1..100 | ForEach-Object { $samples += 1000; $samples += 9000 }
        $s.ObserveMany($samples) | Should -Be 200
        $s.Observe(1000)
        $s.Samples() | Should -Be 201
        $s.TwoGroups() | Should -BeGreaterThan 0
        $s.WirelessConfidence() | Should -BeGreaterOrEqual 0
        $s.Dispose()
    }

    It 'looks for a beat in the delays' {
        $p = New-SubEthaPeriodicity
        $p.GetType().FullName | Should -Be 'SubEtha.Periodicity'
        $null -eq $p.Period() | Should -BeTrue
        $null -eq $p.SecondsToNext() | Should -BeTrue
        0..255 | ForEach-Object { $p.Observe($(if ($_ % 16 -eq 0) { 5000 } else { 100 }), 10000 * $_) }
        $found = $p.Period()
        if ($null -ne $found) {
            $found.GetType().FullName | Should -Be 'SubEtha.Beat'
            $found.Seconds | Should -BeGreaterThan 0
        }
        $p.Dispose()
    }

    It 'estimates capacity from probe pairs and trains' {
        $c = New-SubEthaCapacity -ProbeBytes 1400
        $c.ProbeBytes | Should -Be 1400
        $null -eq $c.LinkCapacity() | Should -BeTrue
        1..10 | ForEach-Object {
            $c.ObservePair(0, 100000.0 * $_)
            $c.ObservePair(1, 100000.0 * $_ + 100)
        }
        1..20 | ForEach-Object { $c.ObserveTrain(1000000.0 + 200 * $_) }
        $c.PairSamples() | Should -BeGreaterThan 0
        $c.TrainSamples() | Should -BeGreaterThan 0
        $link = $c.LinkCapacity()
        if ($null -ne $link) { $link | Should -BeGreaterThan 0 }
        $rate = $c.TrainRate()
        if ($null -ne $rate) { $rate | Should -BeGreaterThan 0 }
        $c.Reset()
        $c.PairSamples() | Should -Be 0
        $c.Dispose()
    }

    It 'forecasts the next interval' {
        $f = New-SubEthaForecast
        $f.GetType().FullName | Should -Be 'SubEtha.Forecast'
        1..10 | ForEach-Object { $f.Observe(125000, 1.0) }
        $f.MeanRate() | Should -BeGreaterThan 0
        $f.NextRate() | Should -BeGreaterThan 0
        { $f.Observe(1, 0) } | Should -Throw
        $f.Dispose()
    }

    It 'notices the route moving' {
        $p = New-SubEthaPathChanges
        $p.GetType().FullName | Should -Be 'SubEtha.PathChanges'
        $null -eq $p.Last() | Should -BeTrue
        1..10 | ForEach-Object { $p.Observe(64, 0, 5) }
        1..10 | ForEach-Object { $p.Observe(64, 3, 9) }
        $last = $p.Last()
        $last.GetType().FullName | Should -Be 'SubEtha.PathMark'
        $last.Hops | Should -Be 9
        $last.CongestionMark | Should -Be 3
        $p.RouteMovement() | Should -BeGreaterOrEqual 0
        $p.MarkedShare() | Should -BeGreaterThan 0
        $p.Dispose()
    }
}
