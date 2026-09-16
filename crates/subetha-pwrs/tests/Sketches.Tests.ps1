# The structures that summarize a stream in fixed memory.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'sketches'
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'SubEtha.BloomFilter' {
    It 'never says no about something that was added' {
        $size = Measure-SubEthaBloomSize -Items 100 -FalsePositiveRate 0.01
        $size.GetType().FullName | Should -Be 'SubEtha.BloomSize'
        $size.Bits | Should -BeGreaterThan 100
        $size.Hashes | Should -BeGreaterThan 0
        $f = New-SubEthaBloomFilter -Path (Join-Path $script:dir 'bloom') -Bits $size.Bits -Hashes $size.Hashes
        $f.Bits | Should -Be $size.Bits
        $f.Contains('apple') | Should -BeFalse
        $f.Insert('apple')
        $f.InsertMany(@('pear', 'plum')) | Should -Be 2
        $f.Contains('apple') | Should -BeTrue
        $f.ContainsMany(@('pear', 'plum', 'apple')) | Should -Be @($true, $true, $true)
        $f.FalsePositiveRate() | Should -BeGreaterOrEqual 0
        $f.Clear()
        $f.Contains('apple') | Should -BeFalse
        $f.Dispose()
    }

    It 'refuses a size of zero' {
        { New-SubEthaBloomFilter -Path (Join-Path $script:dir 'zero') -Bits 0 -Hashes 1 -ErrorAction Stop } | Should -Throw
        { Measure-SubEthaBloomSize -Items 10 -FalsePositiveRate 2 -ErrorAction Stop } | Should -Throw
    }
}

Describe 'SubEtha.BlockedBloomFilter' {
    It 'keeps the bits of one item in one block' {
        $size = Measure-SubEthaBloomSize -Items 64 -FalsePositiveRate 0.02 -Blocked
        $f = New-SubEthaBlockedBloomFilter -Path (Join-Path $script:dir 'blocked') -Bits $size.Bits -Hashes $size.Hashes
        $f.Blocks | Should -BeGreaterThan 0
        $f.Hashes | Should -Be $size.Hashes
        $f.Insert('one')
        $f.InsertMany(@('two', 'three')) | Should -Be 2
        $f.Contains('two') | Should -BeTrue
        $f.ContainsMany(@('one', 'three')) | Should -Be @($true, $true)
        $f.Flush()
        $f.Dispose()
        $again = New-SubEthaBlockedBloomFilter -Path (Join-Path $script:dir 'blocked') -Bits $size.Bits -Hashes $size.Hashes -Reset
        $again.Contains('one') | Should -BeFalse
        $again.Dispose()
    }
}

Describe 'SubEtha.HyperLogLog' {
    It 'estimates distinct items' {
        $h = New-SubEthaHyperLogLog -Path (Join-Path $script:dir 'hll')
        $h.Precision | Should -Be 14
        $h.Registers | Should -Be 16384
        $items = 0..999 | ForEach-Object { "item-$_" }
        $h.InsertMany($items) | Should -Be 1000
        $h.Insert('item-1')
        $h.Estimate() | Should -BeGreaterThan 900
        $h.Estimate() | Should -BeLessThan 1100
        $h.Reset()
        $h.Estimate() | Should -Be 0
        $h.Flush()
        $h.Dispose()
        { New-SubEthaHyperLogLog -Path (Join-Path $script:dir 'hll-bad') -Precision 2 -ErrorAction Stop } | Should -Throw
    }
}

Describe 'SubEtha.CountMinSketch' {
    It 'never undercounts' {
        $size = Measure-SubEthaSketchSize -Epsilon 0.01 -Delta 0.01
        $size.Depth | Should -BeGreaterThan 0
        $size.Width | Should -BeGreaterThan 0
        $s = New-SubEthaCountMinSketch -Path (Join-Path $script:dir 'cms') -Depth $size.Depth -Width $size.Width
        $s.Depth | Should -Be $size.Depth
        $s.Insert('a')
        $s.InsertN('a', 4)
        $s.InsertMany(@('b', 'b', 'c')) | Should -Be 3
        $s.EstimateCount('a') | Should -BeGreaterOrEqual 5
        $s.EstimateMany(@('b', 'c')) | ForEach-Object { $_ | Should -BeGreaterOrEqual 1 }
        $s.TotalInserts() | Should -Be 8
        $s.Reset()
        $s.TotalInserts() | Should -Be 0
        $s.Dispose()
    }
}

Describe 'SubEtha.Histogram' {
    It 'records into buckets and reads percentiles' {
        $h = New-SubEthaHistogram -Path (Join-Path $script:dir 'hist') -Boundaries @(10, 100, 1000)
        $h.Buckets | Should -Be 4
        $h.Boundaries | Should -Be @(10, 100, 1000)
        $h.Record(5) | Should -Be 0
        $h.Record(50) | Should -Be 1
        $h.RecordMany(@(500, 5000)) | Should -Be 2
        $h.TotalCount() | Should -Be 4
        $h.Counts() | Should -Be @(1, 1, 1, 1)
        $h.Count(2) | Should -Be 1
        $h.Percentile(50) | Should -BeGreaterOrEqual 10
        { $h.Percentile(101) } | Should -Throw
        { New-SubEthaHistogram -Path (Join-Path $script:dir 'bad') -Boundaries @(5, 1) -ErrorAction Stop } | Should -Throw
        $h.Dispose()
    }
}

Describe 'SubEtha.RateLimiter' {
    It 'hands out tokens and refills' {
        # One token a second, so nothing comes back between the calls
        # below and the count is what the calls left.
        $r = New-SubEthaRateLimiter -Path (Join-Path $script:dir 'rate') -Capacity 3 -RefillPerSecond 1
        $r.Capacity | Should -Be 3
        $r.RefillPerSecond | Should -Be 1
        $r.Available() | Should -Be 3
        $r.TryAcquire() | Should -BeTrue
        $r.TryAcquire(2) | Should -BeTrue
        $r.TryAcquire(3) | Should -BeFalse
        $r.Reset()
        $r.Available() | Should -Be 3
        $r.Flush()
        $r.Dispose()
    }
}

Describe 'SubEtha.LruCache' {
    It 'evicts the least recently used and tells looking from touching' {
        $c = New-SubEthaLruCache -Path (Join-Path $script:dir 'lru') -Capacity 2 -KeySize 4 -ValueSize 4
        $c.Capacity | Should -Be 2
        $c.Put('k001', 'v001') | Should -BeFalse
        $c.Put('k002', 'v002') | Should -BeFalse
        $c.Put('k002', 'v00X') | Should -BeTrue
        $c.Count() | Should -Be 2
        Get-SEText $c.Get('k001') | Should -Be 'v001'
        $c.Put('k003', 'v003') | Should -BeFalse
        $c.Count() | Should -Be 2
        $c.Contains('k001') | Should -BeFalse
        $c.Contains('k003') | Should -BeTrue
        $null -eq $c.Get('k001') | Should -BeTrue
        Get-SEText $c.GetAndTouch('k002') | Should -Be 'v00X'
        $c.Touch('k002') | Should -BeTrue
        $c.Touch('none') | Should -BeFalse
        $many = $c.GetMany(@('k002', 'zzzz'))
        Get-SEText $many[0] | Should -Be 'v00X'
        $null -eq $many[1] | Should -BeTrue
        $c.PutMany(@('k004'), @('v004')) | Should -Be 1
        Get-SEText $c.Remove('k004') | Should -Be 'v004'
        $null -eq $c.Remove('k004') | Should -BeTrue
        $c.Dispose()
    }
}
