# The value types that ride beside a pointer.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
}

Describe 'SubEtha.TinyBloom' {
    It 'fits a filter in one word' {
        $b = New-SubEthaTinyBloom
        $b.Bits | Should -Be 0
        $b.Contains('a') | Should -BeFalse
        $b.Insert('a')
        $b.Bits | Should -Not -Be 0
        $b.SetBits() | Should -BeGreaterThan 0
        $b.Contains('a') | Should -BeTrue
        $b.InsertMany(@('b', 'c')) | Should -Be 2
        $b.ContainsMany(@('b', 'c')) | Should -Be @($true, $true)
        $copy = New-SubEthaTinyBloom -Bits $b.Bits
        $copy.Contains('a') | Should -BeTrue
        $seeded = New-SubEthaTinyBloom -Key 'x', 'y'
        $seeded.Contains('y') | Should -BeTrue
        $b.SuggestedCapacity() | Should -Be 8
        $b.FalsePositiveRate(8) | Should -BeGreaterThan 0
        $b.FalsePositiveRate(8) | Should -BeLessThan 1
        $b.Dispose()
    }
}

Describe 'SubEtha.FineBloom' {
    It 'holds more keys in four words' {
        $b = New-SubEthaFineBloom -Key 'a'
        $b.SetBits | Should -BeGreaterThan 0
        $b.Contains('a') | Should -BeTrue
        $b.Contains('zzz') | Should -BeFalse
        $b.InsertMany(@('b', 'c')) | Should -Be 2
        $b.ContainsMany(@('a', 'b', 'c')) | Should -Be @($true, $true, $true)
        $b.SuggestedCapacity() | Should -Be 64
        $b.Dispose()
    }
}

Describe 'SubEtha.Clock' {
    It 'advances, merges and compares' {
        $c = New-SubEthaClock -Physical 100 -Logical 0
        $c.GetType().FullName | Should -Be 'SubEtha.Clock'
        $c.Physical | Should -Be 100
        $same = $c.Advance(100)
        $same.Physical | Should -Be 100
        $same.Logical | Should -Be 1
        $later = $c.Advance(200)
        $later.Physical | Should -Be 200
        $later.Logical | Should -Be 0
        $c.Before($later) | Should -BeTrue
        $later.After($c) | Should -BeTrue
        $c.SameAs((New-SubEthaClock -Physical 100)) | Should -BeTrue
        $c.CompareTo($later) | Should -Be -1
        $later.CompareTo($c) | Should -Be 1
        $c.CompareTo($c) | Should -Be 0
        $merged = $c.Merge($later, 150)
        $merged.After($later) | Should -BeTrue
        $now = New-SubEthaClock -Now
        $now.Physical | Should -BeGreaterThan 1000000000000
        { New-SubEthaClock -Now -Physical 5 -ErrorAction Stop } | Should -Throw
        $c.Dispose()
    }
}

Describe 'SubEtha.CausalClock' {
    It 'tells causation from concurrency' {
        $z = New-SubEthaCausalClock
        $z.Counts.Count | Should -Be 16
        $z.Nodes() | Should -Be 16
        $a = $z.Tick(0)
        $a.Count(0) | Should -Be 1
        $b = $z.Tick(1)
        $a.Compare($b) | Should -Be 'concurrent'
        $a.ConcurrentWith($b) | Should -BeTrue
        $ab = $a.Merge($b)
        $ab.Counts[0] | Should -Be 1
        $ab.Counts[1] | Should -Be 1
        $a.HappenedBefore($ab) | Should -BeTrue
        $ab.Compare($a) | Should -Be 'after'
        $a.Compare($a) | Should -Be 'equal'
        { $a.Tick(16) } | Should -Throw
        $given = New-SubEthaCausalClock -Counts (@(0) * 16)
        $given.Compare($z) | Should -Be 'equal'
        $a.Dispose()
    }
}
