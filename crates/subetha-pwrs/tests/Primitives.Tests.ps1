# The single values and plain containers: Region, Cell, Vec, SharedArc,
# LazyValue and BitVec.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'primitives'
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'SubEtha.Region' {
    It 'allocates slots and reads them back' {
        $r = New-SubEthaRegion -Path (Join-Path $script:dir 'region') -Capacity 8 -SlotSize 5
        $r.GetType().FullName | Should -Be 'SubEtha.Region'
        $r.Capacity | Should -Be 8
        $r.SlotSize | Should -Be 5
        $r.Count() | Should -Be 0
        $i = $r.Allocate((ConvertTo-SEBytes 'hello'))
        $i | Should -Be 0
        $r.Count() | Should -Be 1
        Get-SEText $r.Get($i) | Should -Be 'hello'
        $r.Set($i, 'world')
        Get-SEText $r.Get($i) | Should -Be 'world'
        $r.Dispose()
    }

    It 'snapshots the whole span in one array' {
        $r = New-SubEthaRegion -Path (Join-Path $script:dir 'snap') -Capacity 4 -SlotSize 8
        $null = $r.Allocate('abcdefgh')
        $null = $r.Allocate('12345678')
        $all = $r.Snapshot()
        $all.Length | Should -Be 32
        ConvertFrom-SEBytes $all[0..15] | Should -Be 'abcdefgh12345678'
        $r.Dispose()
    }

    It 'refuses an item that is not exactly a slot' {
        $r = New-SubEthaRegion -Path (Join-Path $script:dir 'long') -Capacity 2 -SlotSize 4
        { $r.Allocate('too long for four') } | Should -Throw
        { $r.Allocate('abc') } | Should -Throw
        $r.Dispose()
    }

    It 'answers to the SE alias' {
        $r = New-SERegion -Path (Join-Path $script:dir 'alias') -Capacity 2 -SlotSize 4
        $r.Capacity | Should -Be 2
        $r.Dispose()
    }
}

Describe 'SubEtha.Cell' {
    It 'steps its version on every write' {
        $path = Join-Path $script:dir 'cell'
        $c = New-SubEthaCell -Path $path -ValueSize 8
        $c.ValueSize | Should -Be 8
        $v0 = $c.Version()
        $c.Set('abcdefgh')
        $c.Version() | Should -BeGreaterThan $v0
        Get-SEText $c.Get() | Should -Be 'abcdefgh'
        $other = Open-SubEthaCell -Path $path -ValueSize 8
        Get-SEText $other.Get() | Should -Be 'abcdefgh'
        $c.Flush()
        $c.Dispose()
        $other.Dispose()
    }
}

Describe 'SubEtha.Vec' {
    It 'pushes, pops, gets and sets' {
        $v = New-SubEthaVec -Path (Join-Path $script:dir 'vec') -Capacity 4 -ElementSize 4
        $v.Capacity | Should -Be 4
        $v.ElementSize | Should -Be 4
        $v.Writable | Should -BeTrue
        $v.Push('aaaa') | Should -Be 0
        $v.Push('bbbb') | Should -Be 1
        $v.Count() | Should -Be 2
        Get-SEText $v.Get(1) | Should -Be 'bbbb'
        $null -eq $v.Get(7) | Should -BeTrue
        $v.Set(0, 'cccc')
        Get-SEText $v.Pop() | Should -Be 'bbbb'
        Get-SEText $v.Pop() | Should -Be 'cccc'
        $null -eq $v.Pop() | Should -BeTrue
        $v.Dispose()
    }

    It 'answers null when full and counts a batch' {
        $v = New-SubEthaVec -Path (Join-Path $script:dir 'full') -Capacity 2 -ElementSize 2
        $v.PushMany(@([byte[]](1, 2), [byte[]](3, 4), [byte[]](5, 6))) | Should -Be 2
        $null -eq $v.Push('xx') | Should -BeTrue
        $v.Clear()
        $v.Count() | Should -Be 0
        $v.Dispose()
    }

    It 'moves ranges packed end to end' {
        $v = New-SubEthaVec -Path (Join-Path $script:dir 'range') -Capacity 8 -ElementSize 2
        $null = $v.PushMany(@('ab', 'cd', 'ef', 'gh'))
        $packed = $v.ReadRange(1, 2)
        ConvertFrom-SEBytes $packed | Should -Be 'cdef'
        $v.WriteRange(0, (ConvertTo-SEBytes 'wxyz')) | Should -Be 2
        ConvertFrom-SEBytes $v.ReadRange(0, 2) | Should -Be 'wxyz'
        { $v.WriteRange(0, (ConvertTo-SEBytes 'abc')) } | Should -Throw
        $v.Dispose()
    }
}

Describe 'SubEtha.SharedArc' {
    It 'shares a value between holders and counts them' {
        $path = Join-Path $script:dir 'arc'
        $a = New-SubEthaSharedArc -Path $path -Value 'shared!!' -KeepOnLast
        $a.ValueSize | Should -Be 8
        $a.Holders() | Should -Be 1
        $b = Open-SubEthaSharedArc -Path $path -ValueSize 8 -KeepOnLast
        $a.Holders() | Should -Be 2
        Get-SEText $b.Get() | Should -Be 'shared!!'
        $b.WriteAt(0, 'SH')
        Get-SEText $a.ReadAt(0, 2) | Should -Be 'SH'
        $b.Dispose()
        $a.Holders() | Should -Be 1
        $a.Dispose()
    }
}

Describe 'SubEtha.LazyValue' {
    It 'is claimed, published and read' {
        $l = New-SubEthaLazyValue -Path (Join-Path $script:dir 'lazy') -ValueSize 4
        $l.Ready() | Should -BeFalse
        $null -eq $l.Get() | Should -BeTrue
        $l.Claim() | Should -BeTrue
        $l.Claim() | Should -BeFalse
        { $l.Publish('toolong') } | Should -Throw
        $l.Publish('done') | Should -BeTrue
        $l.Ready() | Should -BeTrue
        Get-SEText $l.Get() | Should -Be 'done'
        Get-SEText $l.Wait(1) | Should -Be 'done'
        $l.Dispose()
    }
}

Describe 'SubEtha.BitVec' {
    It 'sets, clears, toggles and reads bits' {
        $b = New-SubEthaBitVec -Path (Join-Path $script:dir 'bits') -CapacityBits 128
        $b.CapacityBits | Should -Be 128
        $b.Get(5) | Should -BeFalse
        $b.Set(5) | Should -BeFalse
        $b.Get(5) | Should -BeTrue
        $b.Set(5) | Should -BeTrue
        $b.Clear(5) | Should -BeTrue
        $b.Toggle(70) | Should -BeTrue
        $b.Toggle(70) | Should -BeFalse
        $b.SetRange(10, 14)
        $b.Get(13) | Should -BeTrue
        $b.Get(14) | Should -BeFalse
        { $b.Get(1000) } | Should -Throw
        $b.Dispose()
    }
}
