# Every Open- cmdlet, every -Open and -Reset form, and the type each
# structure reports: a structure made by New- is attached to through a
# second handle and read back, and a path that holds nothing is refused.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'open'
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'Open- on the primitives' {
    It 'Open-SubEthaRegion' {
        $p = Join-Path $script:dir 'region'
        $a = New-SubEthaRegion -Path $p -Capacity 4 -SlotSize 4
        $null = $a.Allocate('abcd')
        $b = Open-SubEthaRegion -Path $p -Capacity 4 -SlotSize 4
        $b.GetType().FullName | Should -Be 'SubEtha.Region'
        Get-SEText $b.Get(0) | Should -Be 'abcd'
        $b.Count() | Should -Be 1
        { Open-SubEthaRegion -Path (Join-Path $script:dir 'none') -Capacity 4 -SlotSize 4 -ErrorAction Stop } | Should -Throw
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaVec' {
        $p = Join-Path $script:dir 'vec'
        $a = New-SubEthaVec -Path $p -Capacity 4 -ElementSize 2
        $null = $a.Push('ab')
        $b = Open-SubEthaVec -Path $p -Capacity 4 -ElementSize 2
        $b.GetType().FullName | Should -Be 'SubEtha.Vec'
        $b.Count() | Should -Be 1
        Get-SEText $b.Get(0) | Should -Be 'ab'
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaLazyValue' {
        $p = Join-Path $script:dir 'lazy'
        $a = New-SubEthaLazyValue -Path $p -ValueSize 4
        $null = $a.Claim()
        $null = $a.Publish('done')
        $b = Open-SubEthaLazyValue -Path $p -ValueSize 4
        $b.GetType().FullName | Should -Be 'SubEtha.LazyValue'
        $b.Ready() | Should -BeTrue
        Get-SEText $b.Get() | Should -Be 'done'
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaBitVec' {
        $p = Join-Path $script:dir 'bits'
        $a = New-SubEthaBitVec -Path $p -CapacityBits 64
        $null = $a.Set(3)
        $b = Open-SubEthaBitVec -Path $p -CapacityBits 64
        $b.GetType().FullName | Should -Be 'SubEtha.BitVec'
        $b.Get(3) | Should -BeTrue
        $b.Get(4) | Should -BeFalse
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaCell keeps the type name' {
        $p = Join-Path $script:dir 'cell'
        $a = New-SubEthaCell -Path $p -ValueSize 2
        $a.GetType().FullName | Should -Be 'SubEtha.Cell'
        $a.Set('hi')
        $b = Open-SubEthaCell -Path $p -ValueSize 2
        Get-SEText $b.Get() | Should -Be 'hi'
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaSharedArc keeps the type name' {
        $p = Join-Path $script:dir 'arc'
        $a = New-SubEthaSharedArc -Path $p -Value 'four' -KeepOnLast
        $a.GetType().FullName | Should -Be 'SubEtha.SharedArc'
        $b = Open-SubEthaSharedArc -Path $p -ValueSize 4 -KeepOnLast
        Get-SEText $b.Get() | Should -Be 'four'
        $b.Dispose(); $a.Dispose()
    }
}

Describe 'Open- on the rings' {
    It 'Open-SubEthaSpscRing' {
        $p = Join-Path $script:dir 'spsc'
        $a = New-SubEthaSpscRing -Path $p -Capacity 8
        $null = $a.Push('x')
        $b = Open-SubEthaSpscRing -Path $p -Capacity 8
        $b.GetType().FullName | Should -Be 'SubEtha.SpscRing'
        ConvertFrom-SEBytes $b.Pop() | Should -Be 'x'
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaBroadcastRing' {
        $p = Join-Path $script:dir 'bcast'
        $a = New-SubEthaBroadcastRing -Path $p -Capacity 8
        $b = Open-SubEthaBroadcastRing -Path $p -Capacity 8
        $b.GetType().FullName | Should -Be 'SubEtha.BroadcastRing'
        $c = $b.RegisterConsumer()
        $null = $a.Push('y')
        ConvertFrom-SEBytes $b.Recv($c) | Should -Be 'y'
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaCapacityRing' {
        $p = Join-Path $script:dir 'cap'
        $a = New-SubEthaCapacityRing -Path $p -Capacity 8
        $b = Open-SubEthaCapacityRing -Path $p -Capacity 8
        $b.GetType().FullName | Should -Be 'SubEtha.CapacityRing'
        $prod = $a.RegisterProducer()
        $cons = $b.RegisterConsumer()
        $null = $a.Send($prod, 'z')
        ConvertFrom-SEBytes $b.Recv($cons) | Should -Be 'z'
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaLocaleRing' {
        $p = Join-Path $script:dir 'locale'
        $a = New-SubEthaLocaleRing -Path $p -Capacity 8
        $b = Open-SubEthaLocaleRing -Path $p -Capacity 8
        $b.GetType().FullName | Should -Be 'SubEtha.LocaleRing'
        $b.Locale() | Should -Be $a.Locale()
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaRing with the stamps it was made with' {
        $p = Join-Path $script:dir 'ring'
        $a = New-SubEthaRing -Path $p -Capacity 8 -Stamps Counter
        $b = Open-SubEthaRing -Path $p -Capacity 8 -Stamps Counter
        $b.GetType().FullName | Should -Be 'SubEtha.Ring'
        $b.Stamps | Should -Be ([SubEtha.StampKind]::Counter)
        $prod = $a.RegisterProducer()
        $cons = $b.RegisterConsumer()
        $null = $a.Send($prod, 'w')
        ConvertFrom-SEBytes $b.Recv($cons) | Should -Be 'w'
        { Open-SubEthaRing -Path (Join-Path $script:dir 'none-ring') -Capacity 8 -ErrorAction Stop } | Should -Throw
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaStack' {
        $p = Join-Path $script:dir 'stack'
        $a = New-SubEthaStack -Path $p -Capacity 4 -ElementSize 4
        $null = $a.Push('abcd')
        $b = Open-SubEthaStack -Path $p -Capacity 4 -ElementSize 4
        $b.GetType().FullName | Should -Be 'SubEtha.Stack'
        Get-SEText $b.Pop() | Should -Be 'abcd'
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaPubSub' {
        $p = Join-Path $script:dir 'pubsub'
        $a = New-SubEthaPubSub -Path $p -Capacity 4
        $null = $a.Publish('m')
        $b = Open-SubEthaPubSub -Path $p -Capacity 4
        $b.GetType().FullName | Should -Be 'SubEtha.PubSub'
        $b.Head() | Should -Be 1
        Get-SEText $b.ReadAt(0) | Should -Be 'm'
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaFrameRegion' {
        $p = Join-Path $script:dir 'frames'
        $a = New-SubEthaFrameRegion -Path $p -BlockSize 32 -BlockCount 2
        $i = $a.WriteNew('payload')
        $b = Open-SubEthaFrameRegion -Path $p -BlockSize 32 -BlockCount 2
        $b.GetType().FullName | Should -Be 'SubEtha.FrameRegion'
        Get-SEText $b.ReadBlock($i, 7) | Should -Be 'payload'
        $b.Dispose(); $a.Dispose()
    }

    It 'New-SubEthaLamportPair -Open' {
        $p = Join-Path $script:dir 'lamport'
        $first = New-SubEthaLamportPair -Path $p -Capacity 8
        $null = $first[0].Push('a')
        $first[0].Dispose(); $first[1].Dispose()
        $again = New-SubEthaLamportPair -Path $p -Capacity 8 -Open
        $again.Count | Should -Be 2
        ConvertFrom-SEBytes $again[1].Pop() | Should -Be 'a'
        $again[0].Dispose(); $again[1].Dispose()
    }

    It 'New-SubEthaMpscPool -Open and New-SubEthaMpmcGrid -Open' {
        $p = Join-Path $script:dir 'mpsc'
        $pool = New-SubEthaMpscPool -Path $p -Producers 2 -Capacity 8
        $null = $pool.Producers[1].Push('b')
        $pool.Consumer.Dispose(); $pool.Producers | ForEach-Object { $_.Dispose() }
        $again = New-SubEthaMpscPool -Path $p -Producers 2 -Capacity 8 -Open
        ConvertFrom-SEBytes $again.Consumer.Pop() | Should -Be 'b'
        $again.Consumer.Dispose(); $again.Producers | ForEach-Object { $_.Dispose() }

        $g = Join-Path $script:dir 'mpmc'
        $grid = New-SubEthaMpmcGrid -Path $g -Producers 2 -Consumers 1 -Capacity 8
        $grid.GetType().FullName | Should -Be 'SubEtha.MpmcGrid'
        $null = $grid.Producers[0].Push('c')
        $grid.Consumers | ForEach-Object { $_.Dispose() }; $grid.Producers | ForEach-Object { $_.Dispose() }
        $opened = New-SubEthaMpmcGrid -Path $g -Producers 2 -Consumers 1 -Capacity 8 -Open
        ConvertFrom-SEBytes $opened.Consumers[0].Pop() | Should -Be 'c'
        $opened.Consumers | ForEach-Object { $_.Dispose() }; $opened.Producers | ForEach-Object { $_.Dispose() }
    }
}

Describe 'Open- on the coordination structures' {
    It 'Open-SubEthaLeaderElection' {
        $p = Join-Path $script:dir 'election'
        $a = New-SubEthaLeaderElection -Path $p
        $a.GetType().FullName | Should -Be 'SubEtha.LeaderElection'
        $null = $a.TryClaim()
        $b = Open-SubEthaLeaderElection -Path $p
        $b.Leader() | Should -Be $PID
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaHolderTable' {
        $p = Join-Path $script:dir 'holders'
        $a = New-SubEthaHolderTable -Path $p -Capacity 2
        $a.GetType().FullName | Should -Be 'SubEtha.HolderTable'
        $slot = $a.Claim(9)
        $b = Open-SubEthaHolderTable -Path $p -Capacity 2
        $b.Payload($slot) | Should -Be 9
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaHeartbeat and New-SubEthaEpochBarrier' {
        $p = Join-Path $script:dir 'heartbeat'
        $a = New-SubEthaHeartbeat -Path $p -Capacity 2
        $a.GetType().FullName | Should -Be 'SubEtha.Heartbeat'
        $slot = $a.Register()
        $a.Beat($slot)
        $b = Open-SubEthaHeartbeat -Path $p -Capacity 2
        $snap = $b.Snapshot($slot)
        $snap.GetType().FullName | Should -Be 'SubEtha.HeartbeatSlot'
        $snap.Pid | Should -Be $PID
        $barrier = New-SubEthaEpochBarrier -Path (Join-Path $script:dir 'barrier') -HeartbeatPath $p -Capacity 2
        $barrier.LivePeers() | Should -Be 1
        $barrier.Wait($barrier.CurrentEpoch(), 2) | Should -BeTrue
        $barrier.Dispose(); $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaCondvar' {
        $p = Join-Path $script:dir 'condvar'
        $a = New-SubEthaCondvar -Path $p
        $a.GetType().FullName | Should -Be 'SubEtha.Condvar'
        $g = $a.Generation()
        $b = Open-SubEthaCondvar -Path $p
        $null = $b.NotifyAll()
        $a.Generation() | Should -BeGreaterThan $g
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaFenceClock' {
        $p = Join-Path $script:dir 'fence'
        $a = New-SubEthaFenceClock -Path $p -Capacity 2
        $a.GetType().FullName | Should -Be 'SubEtha.FenceClock'
        $slot = $a.Register()
        $t = $a.Tick($slot)
        $b = Open-SubEthaFenceClock -Path $p -Capacity 2
        $b.GlobalFence().PhysicalUs | Should -BeGreaterOrEqual $t.PhysicalUs
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaEpochs' {
        $p = Join-Path $script:dir 'epochs'
        $a = New-SubEthaEpochs -Path $p -Capacity 2
        $a.GetType().FullName | Should -Be 'SubEtha.Epochs'
        $null = $a.Advance()
        $b = Open-SubEthaEpochs -Path $p -Capacity 2
        $b.Now() | Should -BeGreaterOrEqual 1
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaNotifierSet through a second set on the same file' {
        $p = Join-Path $script:dir 'notify'
        $a = New-SubEthaNotifierSet -Path $p
        $a.GetType().FullName | Should -Be 'SubEtha.NotifierSet'
        $n = $a.Attach()
        $n.GetType().FullName | Should -Be 'SubEtha.Notifier'
        $b = New-SubEthaNotifierSet -Path $p
        $null = $b.Signal()
        $n.Wait(1) | Should -BeTrue
        $n.Dispose(); $b.Dispose(); $a.Dispose()
    }
}

Describe 'Open- on the locks' {
    It 'keeps the type names' {
        $lock = New-SubEthaRWLock -Path (Join-Path $script:dir 'rw')
        $lock.GetType().FullName | Should -Be 'SubEtha.RWLock'
        $sem = New-SubEthaSemaphore -Path (Join-Path $script:dir 'sem') -Initial 1
        $sem.GetType().FullName | Should -Be 'SubEtha.Semaphore'
        $lease = New-SubEthaOwnerLease -Path (Join-Path $script:dir 'lease')
        $lease.GetType().FullName | Should -Be 'SubEtha.OwnerLease'
        $lease.Dispose(); $sem.Dispose(); $lock.Dispose()
    }
}

Describe 'Open- on the sketches' {
    It 'Open-SubEthaBloomFilter and Open-SubEthaBlockedBloomFilter' {
        $p = Join-Path $script:dir 'bloom'
        $a = New-SubEthaBloomFilter -Path $p -Bits 1024 -Hashes 3
        $a.GetType().FullName | Should -Be 'SubEtha.BloomFilter'
        $a.Insert('a')
        $b = Open-SubEthaBloomFilter -Path $p -Bits 1024 -Hashes 3
        $b.Contains('a') | Should -BeTrue
        $b.Dispose(); $a.Dispose()
        $q = Join-Path $script:dir 'blocked'
        $c = New-SubEthaBlockedBloomFilter -Path $q -Bits 4096 -Hashes 4
        $c.GetType().FullName | Should -Be 'SubEtha.BlockedBloomFilter'
        $c.Insert('b')
        $d = Open-SubEthaBlockedBloomFilter -Path $q -Bits 4096 -Hashes 4
        $d.Contains('b') | Should -BeTrue
        $d.Dispose(); $c.Dispose()
    }

    It 'Open-SubEthaHyperLogLog and Open-SubEthaCountMinSketch' {
        $p = Join-Path $script:dir 'hll'
        $a = New-SubEthaHyperLogLog -Path $p
        $a.GetType().FullName | Should -Be 'SubEtha.HyperLogLog'
        $null = $a.InsertMany((1..100 | ForEach-Object { "k$_" }))
        $b = Open-SubEthaHyperLogLog -Path $p
        $b.Estimate() | Should -BeGreaterThan 50
        $b.Dispose(); $a.Dispose()
        $q = Join-Path $script:dir 'cms'
        $c = New-SubEthaCountMinSketch -Path $q -Depth 4 -Width 64
        $c.GetType().FullName | Should -Be 'SubEtha.CountMinSketch'
        $c.InsertN('a', 5)
        $d = Open-SubEthaCountMinSketch -Path $q -Depth 4 -Width 64
        $d.EstimateCount('a') | Should -BeGreaterOrEqual 5
        $d.Dispose(); $c.Dispose()
    }

    It 'Open-SubEthaHistogram, Open-SubEthaRateLimiter and Open-SubEthaLruCache' {
        $p = Join-Path $script:dir 'hist'
        $a = New-SubEthaHistogram -Path $p -Boundaries @(10, 100)
        $a.GetType().FullName | Should -Be 'SubEtha.Histogram'
        $null = $a.Record(5)
        $b = Open-SubEthaHistogram -Path $p -Boundaries @(10, 100)
        $b.TotalCount() | Should -Be 1
        $b.Dispose(); $a.Dispose()
        $q = Join-Path $script:dir 'rate'
        $c = New-SubEthaRateLimiter -Path $q -Capacity 3 -RefillPerSecond 1
        $c.GetType().FullName | Should -Be 'SubEtha.RateLimiter'
        $null = $c.TryAcquire()
        $d = Open-SubEthaRateLimiter -Path $q -Capacity 3 -RefillPerSecond 1
        $d.Available() | Should -Be 2
        $d.Dispose(); $c.Dispose()
        $r = Join-Path $script:dir 'lru'
        $e = New-SubEthaLruCache -Path $r -Capacity 2 -KeySize 2 -ValueSize 2
        $e.GetType().FullName | Should -Be 'SubEtha.LruCache'
        $null = $e.Put('k1', 'v1')
        $f = Open-SubEthaLruCache -Path $r -Capacity 2 -KeySize 2 -ValueSize 2
        Get-SEText $f.Get('k1') | Should -Be 'v1'
        $f.Dispose(); $e.Dispose()
    }
}

Describe 'Open- on the containers' {
    It 'Open-SubEthaLinkedList, Open-SubEthaBTreeMap and Open-SubEthaHashMap' {
        $p = Join-Path $script:dir 'list'
        $a = New-SubEthaLinkedList -Path $p -Capacity 4 -ElementSize 2
        $a.GetType().FullName | Should -Be 'SubEtha.LinkedList'
        $null = $a.PushBack('ab')
        $b = Open-SubEthaLinkedList -Path $p -Capacity 4 -ElementSize 2
        $b.Count() | Should -Be 1
        Get-SEText $b.PopFront() | Should -Be 'ab'
        $b.Dispose(); $a.Dispose()
        $q = Join-Path $script:dir 'btree'
        $c = New-SubEthaBTreeMap -Path $q -Capacity 8 -KeySize 2 -ValueSize 2
        $c.GetType().FullName | Should -Be 'SubEtha.BTreeMap'
        $null = $c.Insert('aa', 'bb')
        $d = Open-SubEthaBTreeMap -Path $q -Capacity 8 -KeySize 2 -ValueSize 2
        Get-SEText $d.Get('aa') | Should -Be 'bb'
        $d.Dispose(); $c.Dispose()
        $r = Join-Path $script:dir 'hash'
        $e = New-SubEthaHashMap -Path $r -Capacity 8 -KeySize 2 -ValueSize 2
        $e.GetType().FullName | Should -Be 'SubEtha.HashMap'
        $null = $e.Insert('cc', 'dd')
        $f = Open-SubEthaHashMap -Path $r -Capacity 8 -KeySize 2 -ValueSize 2
        Get-SEText $f.Get('cc') | Should -Be 'dd'
        { Open-SubEthaHashMap -Path (Join-Path $script:dir 'none-hash') -Capacity 8 -KeySize 2 -ValueSize 2 -ErrorAction Stop } | Should -Throw
        $f.Dispose(); $e.Dispose()
    }

    It 'keeps the type names of the arena and the slab' {
        $arena = New-SubEthaArena -Path (Join-Path $script:dir 'arena') -CapacityBytes 256
        $arena.GetType().FullName | Should -Be 'SubEtha.Arena'
        $slab = New-SubEthaSlab -Path (Join-Path $script:dir 'slab') -Capacity 2 -ElementSize 2
        $slab.GetType().FullName | Should -Be 'SubEtha.Slab'
        $slab.Dispose(); $arena.Dispose()
    }
}

Describe 'Open- on the versioned structures' {
    It 'Open-SubEthaReservoir and New-SubEthaHandleTable -Reset' {
        $p = Join-Path $script:dir 'reservoir'
        $a = New-SubEthaReservoir -Path $p -Capacity 4
        $a.GetType().FullName | Should -Be 'SubEtha.Reservoir'
        $null = $a.Record('a')
        $b = Open-SubEthaReservoir -Path $p -Capacity 4
        $b.Count() | Should -Be 1
        $b.TotalSeen() | Should -Be 1
        $b.Dispose(); $a.Dispose()
        $q = Join-Path $script:dir 'handles'
        $c = New-SubEthaHandleTable -Path $q -Capacity 2
        $c.GetType().FullName | Should -Be 'SubEtha.HandleTable'
        $h = $c.Insert('v')
        $d = Open-SubEthaHandleTable -Path $q -Capacity 2
        Get-SEText $d.Get($h) | Should -Be 'v'
        $d.Dispose(); $c.Dispose()
        $e = New-SubEthaHandleTable -Path $q -Capacity 2 -Reset
        $e.Count() | Should -Be 0
        $e.Dispose()
    }

    It 'Open-SubEthaTimePointTile and its -Reset' {
        $p = Join-Path $script:dir 'tile'
        $a = New-SubEthaTimePointTile -Path $p
        $a.GetType().FullName | Should -Be 'SubEtha.TimePointTile'
        $lane = $a.Insert(5, 'v')
        $b = Open-SubEthaTimePointTile -Path $p
        $b.Count() | Should -Be 1
        $b.At($lane).Version | Should -Be 5
        $b.Dispose(); $a.Dispose()
        $c = New-SubEthaTimePointTile -Path $p -Reset
        $c.Count() | Should -Be 0
        $c.Dispose()
    }

    It 'Open-SubEthaVersionChain and its -Reset' {
        $p = Join-Path $script:dir 'chain'
        $a = New-SubEthaVersionChain -Path $p -Capacity 4
        $a.GetType().FullName | Should -Be 'SubEtha.VersionChain'
        $a.Push(1, 'v')
        $b = Open-SubEthaVersionChain -Path $p -Capacity 4
        Get-SEText $b.ReadAt(1) | Should -Be 'v'
        $b.Dispose(); $a.Dispose()
        $c = New-SubEthaVersionChain -Path $p -Capacity 4 -Reset
        $c.Count() | Should -Be 0
        $c.Dispose()
    }

    It 'Open-SubEthaVersionedSlab and Open-SubEthaVersionedMap' {
        $p = Join-Path $script:dir 'vslab'
        $e = Join-Path $script:dir 'vslab-epochs'
        $a = New-SubEthaVersionedSlab -Path $p -Capacity 4 -EpochsPath $e
        $a.GetType().FullName | Should -Be 'SubEtha.VersionedSlab'
        $a.Set(0, 'v')
        $b = Open-SubEthaVersionedSlab -Path $p -Capacity 4 -EpochsPath $e
        Get-SEText $b.Get(0) | Should -Be 'v'
        $b.Dispose(); $a.Dispose()
        $q = Join-Path $script:dir 'vmap'
        $f = Join-Path $script:dir 'vmap-epochs'
        $c = New-SubEthaVersionedMap -Path $q -Capacity 64 -EpochsPath $f
        $c.GetType().FullName | Should -Be 'SubEtha.VersionedMap'
        $null = $c.Insert(1, 10)
        $d = Open-SubEthaVersionedMap -Path $q -Capacity 64 -EpochsPath $f
        $d.Get(1) | Should -Be 10
        $d.Dispose(); $c.Dispose()
    }

    It 'Open-SubEthaLanedMap' {
        $p = Join-Path $script:dir 'laned'
        $a = New-SubEthaLanedMap -Directory $p -Lanes 2
        $a.GetType().FullName | Should -Be 'SubEtha.LanedMap'
        $claim = $a.ClaimLane()
        $null = $claim.Insert(7, 70)
        $claim.Release()
        $b = Open-SubEthaLanedMap -Directory $p -Lanes 2
        $b.Get(7) | Should -Be 70
        $b.Dispose(); $a.Dispose()
    }

    It 'Open-SubEthaTopologyMap and its -Reset' {
        $p = Join-Path $script:dir 'topology'
        $a = New-SubEthaTopologyMap -Path $p -Participants 4
        $a.GetType().FullName | Should -Be 'SubEtha.TopologyMap'
        $null = $a.RecordSend(0, 1)
        $b = Open-SubEthaTopologyMap -Path $p -Participants 4
        $b.FanOut(0) | Should -Be 1
        $b.Dispose(); $a.Dispose()
        $c = New-SubEthaTopologyMap -Path $p -Participants 4 -Reset
        $c.TotalSends() | Should -Be 0
        $c.Dispose()
    }

    It 'Open-SubEthaGraph and Open-SubEthaUniversal with its -Reset' {
        $p = Join-Path $script:dir 'graph'
        $a = New-SubEthaGraph -Path $p -MaxNodes 8 -MaxEdges 8
        $a.GetType().FullName | Should -Be 'SubEtha.Graph'
        $n = $a.AddNode(5)
        $b = Open-SubEthaGraph -Path $p -MaxNodes 8 -MaxEdges 8
        $b.NodeValue($n) | Should -Be 5
        { Open-SubEthaGraph -Path (Join-Path $script:dir 'none-graph') -MaxNodes 8 -MaxEdges 8 -ErrorAction Stop } | Should -Throw
        $b.Dispose(); $a.Dispose()
        $q = Join-Path $script:dir 'universal'
        $c = New-SubEthaUniversal -Path $q -Capacity 8
        $c.GetType().FullName | Should -Be 'SubEtha.Universal'
        $c.Insert(3)
        $d = Open-SubEthaUniversal -Path $q -Capacity 8
        $d.Contains(3) | Should -BeTrue
        $d.Dispose(); $c.Dispose()
        $e = New-SubEthaUniversal -Path $q -Capacity 8 -Reset
        $e.Count() | Should -Be 0
        $e.Dispose()
    }

    It 'Open-SubEthaTower' {
        $bottom = Join-Path $script:dir 'tower-bottom'
        $top = Join-Path $script:dir 'tower-top'
        $a = New-SubEthaTower -Path $bottom -Capacity 8 -ValueSize 4 -LevelPath @($top) -LevelCapacity @(8)
        $a.GetType().FullName | Should -Be 'SubEtha.Tower'
        $path = $a.Append('abcd')
        $b = Open-SubEthaTower -Path $bottom -Capacity 8 -ValueSize 4 -LevelPath @($top) -LevelCapacity @(8)
        Get-SEText $b.Get($path) | Should -Be 'abcd'
        $b.Dispose(); $a.Dispose()
    }
}

Describe 'the front door and the values keep their type names' {
    It 'names every object' {
        $ch = New-SubEthaChannel -Path (Join-Path $script:dir 'channel') -Capacity 8
        $ch.GetType().FullName | Should -Be 'SubEtha.Channel'
        $ch.Dispose()
        $q = New-SubEthaAdaptiveQueue -Path (Join-Path $script:dir 'adaptive') -Capacity 8
        $q.GetType().FullName | Should -Be 'SubEtha.AdaptiveQueue'
        $q.Traffic().GetType().FullName | Should -Be 'SubEtha.Traffic'
        $q.Dispose()
        $w = New-SubEthaWorkQueue -Path (Join-Path $script:dir 'work') -Capacity 8
        $w.GetType().FullName | Should -Be 'SubEtha.WorkQueue'
        $w.Dispose()
        $kv = New-SubEthaKvMap -Path (Join-Path $script:dir 'kv') -Capacity 8
        $kv.GetType().FullName | Should -Be 'SubEtha.KvMap'
        $kv.Dispose()
        $policy = New-SubEthaQosPolicy -Preset ([SubEtha.QosPreset]::Streaming)
        $policy.GetType().FullName | Should -Be 'SubEtha.QosPolicy'
        $policy.Dispose()
        (New-SubEthaTinyBloom).GetType().FullName | Should -Be 'SubEtha.TinyBloom'
        (New-SubEthaFineBloom).GetType().FullName | Should -Be 'SubEtha.FineBloom'
        (New-SubEthaCausalClock).GetType().FullName | Should -Be 'SubEtha.CausalClock'
        (Measure-SubEthaSketchSize -Epsilon 0.1 -Delta 0.1).GetType().FullName | Should -Be 'SubEtha.SketchSize'
    }
}
