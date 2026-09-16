# Items through the pipeline: Send-SubEthaItem and Receive-SubEthaItem.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'pipeline'
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'Send-SubEthaItem and Receive-SubEthaItem' {
    It 'pipe through a single-producer ring' {
        $r = New-SubEthaSpscRing -Path (Join-Path $script:dir 'spsc') -Capacity 8
        $refused = 'a', 'b', 'c' | Send-SubEthaItem -To $r
        @($refused).Count | Should -Be 0
        $got = Receive-SubEthaItem -From $r | ForEach-Object { ConvertFrom-SEBytes $_ }
        $got -join '' | Should -Be 'abc'
        @(Receive-SubEthaItem -From $r).Count | Should -Be 0
        $r.Dispose()
    }

    It 'hand the item of a full ring back' {
        $r = New-SubEthaSpscRing -Path (Join-Path $script:dir 'full') -Capacity 2
        $refused = 'a', 'b', 'c', 'd' | Send-SubEthaItem -To $r
        @($refused).Count | Should -BeGreaterThan 0
        $r.Dispose()
    }

    It 'pipe through an adaptive ring with ids' {
        $r = New-SubEthaRing -Path (Join-Path $script:dir 'ring') -Capacity 16
        $p = $r.RegisterProducer()
        $c = $r.RegisterConsumer()
        'x', 'y', 'z' | Send-SEItem -To $r -Producer $p
        $got = Receive-SEItem -From $r -Consumer $c -Count 2 | ForEach-Object { ConvertFrom-SEBytes $_ }
        $got -join '' | Should -Be 'xy'
        (Receive-SubEthaItem -From $r -Consumer $c | ForEach-Object { ConvertFrom-SEBytes $_ }) -join '' | Should -Be 'z'
        $r.Dispose()
    }

    It 'pipe through a channel and a deque' {
        $ch = New-SubEthaChannel -Path (Join-Path $script:dir 'channel') -Capacity 8
        'one', 'two' | Send-SubEthaItem -To $ch
        (Receive-SubEthaItem -From $ch | ForEach-Object { Get-SEText $_ }) -join ',' | Should -Be 'one,two'
        $ch.Dispose()
        $d = New-SubEthaDeque -Path (Join-Path $script:dir 'deque') -Capacity 8 -ElementSize 4
        'aaaa', 'bbbb' | Send-SubEthaItem -To $d
        (Receive-SubEthaItem -From $d -Owner | ForEach-Object { Get-SEText $_ }) -join ',' | Should -Be 'bbbb,aaaa'
        $d.Dispose()
    }

    It 'read a subscriber and refuse a bare pubsub' {
        $p = New-SubEthaPubSub -Path (Join-Path $script:dir 'pubsub') -Capacity 8
        $sub = $p.Subscribe()
        'm', 'n' | Send-SubEthaItem -To $p
        (Receive-SubEthaItem -From $sub | ForEach-Object { Get-SEText $_ }) -join '' | Should -Be 'mn'
        { Receive-SubEthaItem -From $p -ErrorAction Stop } | Should -Throw
        { 'q' | Send-SubEthaItem -To $sub -ErrorAction Stop } | Should -Throw
        $sub.Dispose()
        $p.Dispose()
    }

    It 'refuse a structure that does not move items' {
        $a = New-SubEthaAtomic -Path (Join-Path $script:dir 'atomic')
        { 'x' | Send-SubEthaItem -To $a -ErrorAction Stop } | Should -Throw
        { Receive-SubEthaItem -From $a -ErrorAction Stop } | Should -Throw
        $a.Dispose()
    }
}
