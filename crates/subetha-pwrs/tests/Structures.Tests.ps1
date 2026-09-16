# The plain containers and the pooled rings.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'structures'
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'SubEtha.Arena' {
    It 'interns strings and resolves them by reference' {
        $path = Join-Path $script:dir 'arena'
        $a = New-SubEthaArena -Path $path -CapacityBytes 4096
        $a.Writable | Should -BeTrue
        $a.CapacityBytes | Should -Be 4096
        $r = $a.Intern('hello')
        $a.Get($r) | Should -Be 'hello'
        $rb = $a.InternBytes([byte[]](1, 2, 3))
        $a.GetBytes($rb) | Should -Be @(1, 2, 3)
        $refs = $a.InternMany(@('x', 'y'))
        $refs.Count | Should -Be 2
        $a.GetMany($refs) | Should -Be @('x', 'y')
        $a.UsedBytes() | Should -BeGreaterThan 0
        $a.RemainingBytes() | Should -BeLessThan 4096
        $ro = Open-SubEthaArena -Path $path -CapacityBytes 4096 -ReadOnly
        $ro.Writable | Should -BeFalse
        $ro.Get($r) | Should -Be 'hello'
        $ro.Dispose()
        $a.Dispose()
    }

    It 'answers null when full' {
        $a = New-SubEthaArena -Path (Join-Path $script:dir 'arena-small') -CapacityBytes 64
        $null = $a.Intern('x')
        $null -eq $a.Intern(('y' * 200)) | Should -BeTrue
        $a.Dispose()
    }
}

Describe 'SubEtha.LinkedList' {
    It 'links at both ends and by node index' {
        $l = New-SubEthaLinkedList -Path (Join-Path $script:dir 'list') -Capacity 8 -ElementSize 4
        $l.Count() | Should -Be 0
        $b = $l.PushBack('bbbb')
        $null = $l.PushFront('aaaa')
        $l.PushBackMany(@('cccc', 'dddd')).Count | Should -Be 2
        $l.Count() | Should -Be 4
        Get-SEText $l.Get($b) | Should -Be 'bbbb'
        Get-SEText $l.PopFront() | Should -Be 'aaaa'
        Get-SEText $l.PopBack() | Should -Be 'dddd'
        Get-SEText $l.Remove($b) | Should -Be 'bbbb'
        $l.Count() | Should -Be 1
        Get-SEText $l.PopFront() | Should -Be 'cccc'
        $null -eq $l.PopFront() | Should -BeTrue
        $l.Dispose()
    }
}

Describe 'SubEtha.Slab' {
    It 'reads and writes slots under a seqlock' {
        $path = Join-Path $script:dir 'slab'
        $s = New-SubEthaSlab -Path $path -Capacity 4 -ElementSize 2
        $s.Writable | Should -BeTrue
        $v0 = $s.SlotVersion(1)
        $s.Set(1, 'ab')
        $s.SlotVersion(1) | Should -BeGreaterThan $v0
        Get-SEText $s.Get(1) | Should -Be 'ab'
        $s.WriteRange(2, (ConvertTo-SEBytes 'cdef')) | Should -Be 2
        ConvertFrom-SEBytes $s.ReadRange(1, 3) | Should -Be 'abcdef'
        { $s.Get(9) } | Should -Throw
        $s.Flush()
        $ro = Open-SubEthaSlab -Path $path -Capacity 4 -ElementSize 2 -ReadOnly
        $ro.Writable | Should -BeFalse
        Get-SEText $ro.Get(2) | Should -Be 'cd'
        $ro.Dispose()
        $s.Dispose()
    }
}

Describe 'SubEtha.BTreeMap' {
    It 'keeps keys in byte order' {
        $m = New-SubEthaBTreeMap -Path (Join-Path $script:dir 'btree') -Capacity 16 -KeySize 2 -ValueSize 4
        $m.Count() | Should -Be 0
        $null -eq $m.Insert('bb', 'two_') | Should -BeTrue
        $null -eq $m.Insert('aa', 'one_') | Should -BeTrue
        Get-SEText $m.Insert('bb', 'TWO_') | Should -Be 'two_'
        $m.InsertMany(@('cc', 'dd'), @('thre', 'four')) | Should -Be 2
        $m.Count() | Should -Be 4
        $m.Nodes() | Should -BeGreaterThan 0
        Get-SEText $m.Get('bb') | Should -Be 'TWO_'
        $null -eq $m.Get('zz') | Should -BeTrue
        $m.Contains('cc') | Should -BeTrue
        $m.First().GetType().FullName | Should -Be 'SubEtha.Pair'
        Get-SEText $m.First().Key | Should -Be 'aa'
        Get-SEText $m.Last().Value | Should -Be 'four'
        Get-SEText $m.Remove('aa') | Should -Be 'one_'
        $null -eq $m.Remove('aa') | Should -BeTrue
        $m.Clear()
        $m.Count() | Should -Be 0
        $null -eq $m.First() | Should -BeTrue
        $m.Flush()
        $m.Dispose()
    }
}

Describe 'SubEtha.HashMap' {
    It 'inserts, updates, exchanges and removes' {
        $m = New-SubEthaHashMap -Path (Join-Path $script:dir 'hash') -Capacity 8 -KeySize 4 -ValueSize 4
        $m.Insert('key1', 'val1') | Should -Be ([SubEtha.InsertOutcome]::Inserted)
        $m.Insert('key1', 'val2') | Should -Be ([SubEtha.InsertOutcome]::Updated)
        $m.InsertMany(@('key2', 'key3'), @('v002', 'v003')) | Should -Be 2
        $m.Count() | Should -Be 3
        Get-SEText $m.Get('key1') | Should -Be 'val2'
        $many = $m.GetMany(@('key2', 'none'))
        Get-SEText $many[0] | Should -Be 'v002'
        $null -eq $many[1] | Should -BeTrue
        $m.Contains('key3') | Should -BeTrue
        $x = $m.CompareExchange('key1', 'val2', 'val9')
        $x.GetType().FullName | Should -Be 'SubEtha.Exchange'
        $x.Swapped | Should -BeTrue
        Get-SEText $x.Found | Should -Be 'val2'
        $y = $m.CompareExchange('key1', 'val2', 'val0')
        $y.Swapped | Should -BeFalse
        Get-SEText $y.Found | Should -Be 'val9'
        Get-SEText $m.Remove('key1') | Should -Be 'val9'
        $m.Tombstones() | Should -BeGreaterOrEqual 0
        $m.Clear()
        $m.Count() | Should -Be 0
        $m.Dispose()
    }
}

Describe 'SubEtha.MpscPool' {
    It 'drains every producer through one consumer' {
        $pool = New-SubEthaMpscPool -Path (Join-Path $script:dir 'mpsc') -Producers 2 -Capacity 8
        $pool.GetType().FullName | Should -Be 'SubEtha.MpscPool'
        $pool.Producers.Count | Should -Be 2
        $pool.Producers[0].GetType().FullName | Should -Be 'SubEtha.MpscProducer'
        $pool.Consumer.GetType().FullName | Should -Be 'SubEtha.MpscConsumer'
        $pool.Producers[0].Index | Should -Be 0
        $pool.Producers[1].Capacity | Should -Be 8
        $pool.Consumer.Producers | Should -Be 2
        $pool.Producers[0].Push('a') | Should -BeTrue
        $pool.Producers[1].PushMany(@('b', 'c')) | Should -Be 2
        $pool.Consumer.ApproxLen() | Should -Be 3
        $got = $pool.Consumer.PopMany(10) | ForEach-Object { ConvertFrom-SEBytes $_ } | Sort-Object
        $got -join '' | Should -Be 'abc'
        $null -eq $pool.Consumer.Pop() | Should -BeTrue
        $pool.Consumer.Dispose()
        $pool.Producers | ForEach-Object { $_.Dispose() }
    }
}

Describe 'SubEtha.MpmcGrid' {
    It 'shares producers out among consumers' {
        $grid = New-SubEthaMpmcGrid -Path (Join-Path $script:dir 'mpmc') -Producers 2 -Consumers 2 -Capacity 8
        $grid.Producers.Count | Should -Be 2
        $grid.Consumers.Count | Should -Be 2
        $grid.Producers[0].GetType().FullName | Should -Be 'SubEtha.MpmcProducer'
        $grid.Consumers[0].GetType().FullName | Should -Be 'SubEtha.MpmcConsumer'
        $grid.Consumers[0].Rings | Should -BeGreaterThan 0
        $grid.Producers[0].Push('a') | Should -BeTrue
        $grid.Producers[1].Push('b') | Should -BeTrue
        $all = @()
        foreach ($c in $grid.Consumers) { $all += $c.PopMany(10) | ForEach-Object { ConvertFrom-SEBytes $_ } }
        ($all | Sort-Object) -join '' | Should -Be 'ab'
        { New-SubEthaMpmcGrid -Path (Join-Path $script:dir 'mpmc-bad') -Producers 1 -Consumers 2 -Capacity 8 -ErrorAction Stop } | Should -Throw
        $grid.Consumers | ForEach-Object { $_.Dispose() }
        $grid.Producers | ForEach-Object { $_.Dispose() }
    }
}
