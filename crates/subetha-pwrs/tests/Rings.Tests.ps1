# The rings and the other structures that move items.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'rings'
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'SubEtha.SpscRing' {
    It 'pushes and pops single items' {
        $r = New-SubEthaSpscRing -Path (Join-Path $script:dir 'spsc') -Capacity 8
        $r.GetType().FullName | Should -Be 'SubEtha.SpscRing'
        $r.Capacity | Should -Be 8
        $r.PayloadSize | Should -Be 64
        $null -eq $r.Pop() | Should -BeTrue
        $r.Push('one') | Should -BeTrue
        $r.Push((ConvertTo-SEBytes 'two')) | Should -BeTrue
        ConvertFrom-SEBytes $r.Pop() | Should -Be 'one'
        ConvertFrom-SEBytes $r.Pop() | Should -Be 'two'
        $r.Dispose()
    }

    It 'moves runs of items and reports a full ring' {
        $r = New-SubEthaSpscRing -Path (Join-Path $script:dir 'spsc-many') -Capacity 4
        $items = @('a', 'b', 'c', 'd', 'e', 'f')
        $pushed = $r.PushMany($items)
        $pushed | Should -BeGreaterThan 0
        $pushed | Should -BeLessThan 6
        $r.Push('z') | Should -BeFalse
        $got = $r.PopMany(10)
        $got.Count | Should -Be $pushed
        ConvertFrom-SEBytes $got[0] | Should -Be 'a'
        $r.Dispose()
    }

    It 'moves items packed end to end' {
        $r = New-SubEthaSpscRing -Path (Join-Path $script:dir 'spsc-packed') -Capacity 8
        $r.PushPacked((ConvertTo-SEBytes 'aabbcc'), 2) | Should -Be 3
        $packed = $r.PopPacked(10)
        $packed.GetType().FullName | Should -Be 'SubEtha.PackedItems'
        $packed.Count | Should -Be 3
        $packed.Bytes.Length | Should -Be (3 * 64)
        ConvertFrom-SEBytes $packed.Bytes[64..65] | Should -Be 'bb'
        $r.Dispose()
    }

    It 'refuses an item longer than a slot' {
        $r = New-SubEthaSpscRing -Path (Join-Path $script:dir 'spsc-long') -Capacity 2
        { $r.Push([byte[]]::new(65)) } | Should -Throw
        $r.Dispose()
    }
}

Describe 'SubEtha.BroadcastRing' {
    It 'delivers every item to every consumer' {
        $r = New-SubEthaBroadcastRing -Path (Join-Path $script:dir 'bcast') -Capacity 8
        $a = $r.RegisterConsumer()
        $b = $r.RegisterConsumer()
        $r.ActiveConsumers() | Should -Be 2
        $r.Push('x') | Should -BeTrue
        $r.PushMany(@('y', 'z')) | Should -Be 2
        $r.ProducerPosition() | Should -Be 3
        $r.Lag($a) | Should -Be 3
        ConvertFrom-SEBytes $r.Recv($a) | Should -Be 'x'
        ($r.RecvMany($b, 10) | ForEach-Object { ConvertFrom-SEBytes $_ }) -join '' | Should -Be 'xyz'
        $null -eq $r.Recv($b) | Should -BeTrue
        $r.UnregisterConsumer($a)
        $r.Dispose()
    }
}

Describe 'SubEtha.CapacityRing' {
    It 'sends, receives and resizes without losing items' {
        $r = New-SubEthaCapacityRing -Path (Join-Path $script:dir 'cap') -Capacity 4
        $p = $r.RegisterProducer()
        $c = $r.RegisterConsumer()
        $r.Capacity() | Should -Be 4
        $r.Send($p, 'a') | Should -BeTrue
        $r.Send($p, 'b') | Should -BeTrue
        $generation = $r.PinGeneration()
        $r.MorphTo(16)
        $r.Capacity() | Should -Be 16
        $r.PinGeneration() | Should -BeGreaterThan $generation
        $r.Send($p, 'c') | Should -BeTrue
        ($r.RecvMany($c, 10) | ForEach-Object { ConvertFrom-SEBytes $_ }) -join '' | Should -Be 'abc'
        $r.StalePops() | Should -BeGreaterOrEqual 0
        { New-SubEthaCapacityRing -Path (Join-Path $script:dir 'cap-odd') -Capacity 6 -ErrorAction Stop } | Should -Throw
        $r.Dispose()
    }

    It 'prewarms a backing and reports it' {
        $r = New-SubEthaCapacityRing -Path (Join-Path $script:dir 'warm') -Capacity 4
        $null -eq $r.WarmCapacity() | Should -BeTrue
        $r.Prewarm(8)
        $r.WarmCapacity() | Should -Be 8
        $r.MorphTo(8)
        $r.WarmHits() | Should -Be 1
        $r.ClearWarm()
        $r.Dispose()
    }

    It 'carries an ordering mode when stamped' {
        $r = New-SubEthaCapacityRing -Path (Join-Path $script:dir 'cap-stamped') -Capacity 4 -Stamped
        $r.Stamped | Should -BeTrue
        $r.SetOrderingMode([SubEtha.OrderingMode]::MergeByStamp)
        $r.OrderingMode() | Should -Be ([SubEtha.OrderingMode]::MergeByStamp)
        $r.Inversions() | Should -Be 0
        $r.Dispose()
        $u = New-SubEthaCapacityRing -Path (Join-Path $script:dir 'cap-unstamped') -Capacity 4
        $null -eq $u.OrderingMode() | Should -BeTrue
        $u.Dispose()
    }
}

Describe 'SubEtha.LocaleRing' {
    It 'migrates between locales carrying its items' {
        $r = New-SubEthaLocaleRing -Path (Join-Path $script:dir 'locale') -Capacity 8
        $r.Locale() | Should -Be ([SubEtha.Locale]::Anon)
        $p = $r.RegisterProducer()
        $c = $r.RegisterConsumer()
        $r.Send($p, 'a') | Should -BeTrue
        $g = $r.LocaleGeneration()
        $r.MigrateTo([SubEtha.Locale]::File)
        $r.Locale() | Should -Be ([SubEtha.Locale]::File)
        $r.LocaleGeneration() | Should -BeGreaterThan $g
        ConvertFrom-SEBytes $r.Recv($c) | Should -Be 'a'
        $r.Dispose()
    }
}

Describe 'SubEtha.Ring' {
    It 'sends and receives with registered ids' {
        $r = New-SubEthaRing -Path (Join-Path $script:dir 'ring') -Capacity 16 -MaxProducers 2 -MaxConsumers 2
        $r.GetType().FullName | Should -Be 'SubEtha.Ring'
        $r.MaxProducers | Should -Be 2
        $null -eq $r.Stamps | Should -BeTrue
        $r.Stamped() | Should -BeFalse
        $p = $r.RegisterProducer()
        $c = $r.RegisterConsumer()
        $r.IsEmpty() | Should -BeTrue
        $r.Send($p, 'hello') | Should -BeTrue
        $r.ApproxLen() | Should -Be 1
        $r.Shape() | Should -Not -BeNullOrEmpty
        $r.TotalCapacity() | Should -BeGreaterOrEqual $r.Capacity()
        ConvertFrom-SEBytes $r.Recv($c) | Should -Be 'hello'
        $r.SendMany($p, @('a', 'b')) | Should -Be 2
        $r.SendPacked($p, (ConvertTo-SEBytes 'cd'), 1) | Should -Be 2
        ($r.RecvMany($c, 10) | ForEach-Object { ConvertFrom-SEBytes $_ }) -join '' | Should -Be 'abcd'
        $r.MorphRefusals() | Should -BeGreaterOrEqual 0
        $r.Dispose()
    }

    It 'carries a frame longer than a slot' {
        $r = New-SubEthaRing -Path (Join-Path $script:dir 'frames') -Capacity 64
        $p = $r.RegisterProducer()
        $c = $r.RegisterConsumer()
        $big = [byte[]]::new(300)
        for ($i = 0; $i -lt 300; $i++) { $big[$i] = $i % 251 }
        $r.SendFrame($p, $big) | Should -BeTrue
        $got = $r.RecvFrame($c)
        $got.Length | Should -Be 300
        $got[299] | Should -Be (299 % 251)
        $null -eq $r.RecvFrame($c) | Should -BeTrue
        $r.Dispose()
    }

    It 'delivers stamped items in order through an ordered receiver' {
        $r = New-SubEthaRing -Path (Join-Path $script:dir 'ordered') -Capacity 64 -MaxProducers 2 -Stamps Counter
        $r.Stamps | Should -Be ([SubEtha.StampKind]::Counter)
        $r.Stamped() | Should -BeTrue
        $p1 = $r.RegisterProducer()
        $p2 = $r.RegisterProducer()
        $c = $r.RegisterConsumer()
        $recv = $r.OrderedReceiver($c)
        $recv.GetType().FullName | Should -Be 'SubEtha.OrderedReceiver'
        $null = $r.Send($p1, 'a')
        $null = $r.Send($p2, 'b')
        $null = $r.Send($p1, 'c')
        $items = $recv.Drain(100)
        $items.Count | Should -Be 3
        $items[0].GetType().FullName | Should -Be 'SubEtha.StampedItem'
        ($items | ForEach-Object { ConvertFrom-SEBytes $_.Bytes }) -join '' | Should -Be 'abc'
        ($items | ForEach-Object { $_.Stamp }) | Sort-Object | Should -Be ($items | ForEach-Object { $_.Stamp })
        $recv.Strategy() | Should -Not -BeNullOrEmpty
        $recv.Corrections() | Should -BeGreaterOrEqual 0
        # The receiver keeps the ring alive on its own: disposing the
        # ring object first leaves the receiver usable.
        $recv.RingShared() | Should -BeTrue
        $r.Dispose()
        $recv.RingShared() | Should -BeFalse
        @($recv.FlushAll()).Count | Should -Be 0
        $recv.Dispose()
    }

    It 'refuses an ordered receiver on an unstamped ring' {
        $r = New-SubEthaRing -Path (Join-Path $script:dir 'unstamped') -Capacity 8
        $c = $r.RegisterConsumer()
        { $r.OrderedReceiver($c) } | Should -Throw
        $r.Dispose()
    }
}

Describe 'SubEtha.ReorderWindow' {
    It 'puts items back in stamp order' {
        $w = New-SubEthaReorderWindow -Floor 2 -Cap 16
        $w.Floor | Should -Be 2
        $w.Push(3, 'c')
        $w.Push(1, 'a')
        $w.Push(2, 'b')
        $w.Count() | Should -Be 3
        $first = $w.Take()
        $first.Stamp | Should -Be 1
        $rest = $w.FlushAll()
        ($rest | ForEach-Object { $_.Stamp }) | Should -Be @(2, 3)
        $w.Count() | Should -Be 0
        $w.PushMany(@([uint64] 9, [uint64] 8), @('i', 'h')) | Should -Be 2
        $w.WidenTo(4)
        $w.Window() | Should -BeGreaterOrEqual 4
        $w.Dispose()
    }
}

Describe 'SubEtha.Stack' {
    It 'is last in first out' {
        $s = New-SubEthaStack -Path (Join-Path $script:dir 'stack') -Capacity 4 -ElementSize 8
        $s.IsEmpty() | Should -BeTrue
        $s.Push('one') | Should -BeTrue
        $s.PushMany(@('two', 'three')) | Should -Be 2
        $s.ApproxLen() | Should -Be 3
        Get-SEText $s.Peek() | Should -Be 'three'
        Get-SEText $s.Pop() | Should -Be 'three'
        ($s.PopMany(10) | ForEach-Object { Get-SEText $_ }) -join ',' | Should -Be 'two,one'
        $null -eq $s.Pop() | Should -BeTrue
        $s.Dispose()
    }
}

Describe 'SubEtha.Deque' {
    It 'is popped by its owner and stolen by a thief' {
        $path = Join-Path $script:dir 'deque'
        $owner = New-SubEthaDeque -Path $path -Capacity 8 -ElementSize 4
        $owner.Thief | Should -BeFalse
        $owner.PushMany(@('a', 'b', 'c')) | Should -Be 3
        $thief = Open-SubEthaDeque -Path $path -ElementSize 4
        $thief.Thief | Should -BeTrue
        $thief.Capacity | Should -Be 8
        Get-SEText $thief.Steal() | Should -Be 'a'
        Get-SEText $owner.Pop() | Should -Be 'c'
        ($thief.StealMany(10) | ForEach-Object { Get-SEText $_ }) -join '' | Should -Be 'b'
        $thief.Dispose()
        $owner.Dispose()
    }
}

Describe 'SubEtha.PubSub' {
    It 'publishes to subscribers that read at their own pace' {
        $p = New-SubEthaPubSub -Path (Join-Path $script:dir 'pubsub') -Capacity 4
        $p.Head() | Should -Be 0
        $sub = $p.Subscribe()
        $sub.GetType().FullName | Should -Be 'SubEtha.Subscriber'
        $p.Publish('a') | Should -Be 0
        $p.PublishMany(@('b', 'c')) | Should -Be 2
        $sub.Lag() | Should -Be 3
        Get-SEText $sub.Next() | Should -Be 'a'
        ($sub.NextMany(10) | ForEach-Object { Get-SEText $_ }) -join '' | Should -Be 'bc'
        $null -eq $sub.Next() | Should -BeTrue
        $sub.Position() | Should -Be 3
        Get-SEText $p.ReadAt(1) | Should -Be 'b'
        $null -eq $p.ReadAt(3) | Should -BeTrue
        $replay = $p.SubscribeFrom(2)
        Get-SEText $replay.Next() | Should -Be 'c'
        $replay.Dispose()
        $sub.Dispose()
        $p.Dispose()
    }

    It 'reports a subscriber that fell behind' {
        $p = New-SubEthaPubSub -Path (Join-Path $script:dir 'lagged') -Capacity 2
        $sub = $p.Subscribe()
        $null = $p.PublishMany(@('a', 'b', 'c', 'd'))
        { $sub.Next() } | Should -Throw
        $sub.Position() | Should -Be 4
        $sub.Dispose()
        $p.Dispose()
    }
}

Describe 'SubEtha.LamportPair' {
    It 'hands out one producer and one consumer' {
        $pair = New-SubEthaLamportPair -Path (Join-Path $script:dir 'lamport') -Capacity 8
        $pair.Count | Should -Be 2
        $prod = $pair[0]
        $cons = $pair[1]
        $prod.GetType().FullName | Should -Be 'SubEtha.LamportProducer'
        $cons.GetType().FullName | Should -Be 'SubEtha.LamportConsumer'
        $prod.Push('a') | Should -BeTrue
        $prod.PushMany(@('b', 'c')) | Should -Be 2
        $prod.PushPacked((ConvertTo-SEBytes 'de'), 1) | Should -Be 2
        ($cons.PopMany(10) | ForEach-Object { ConvertFrom-SEBytes $_ }) -join '' | Should -Be 'abcde'
        $null -eq $cons.Pop() | Should -BeTrue
        $prod.Dispose()
        $cons.Dispose()
    }
}

Describe 'SubEtha.FrameRegion' {
    It 'allocates, writes, reads and frees blocks' {
        $f = New-SubEthaFrameRegion -Path (Join-Path $script:dir 'frames-pool') -BlockSize 32 -BlockCount 2
        $f.BlockSize | Should -Be 32
        $f.BlockCount | Should -Be 2
        $i = $f.WriteNew('payload')
        $null -ne $i | Should -BeTrue
        Get-SEText $f.ReadBlock($i, 7) | Should -Be 'payload'
        $j = $f.Allocate()
        $null -ne $j | Should -BeTrue
        $null -eq $f.Allocate() | Should -BeTrue
        $f.WriteBlock($j, 'second')
        Get-SEText $f.TakeBlock($j, 6) | Should -Be 'second'
        $null -ne $f.Allocate() | Should -BeTrue
        { $f.WriteBlock($i, [byte[]]::new(40)) } | Should -Throw
        $f.Free($i)
        $f.Dispose()
    }
}
