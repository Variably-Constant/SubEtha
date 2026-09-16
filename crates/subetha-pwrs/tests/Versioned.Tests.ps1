# The structures that keep history or reach values by something other
# than a place.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'versioned'
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'SubEtha.Reservoir' {
    It 'keeps a bounded sample' {
        $r = New-SubEthaReservoir -Path (Join-Path $script:dir 'reservoir') -Capacity 4
        $r.MaxValueBytes | Should -Be 52
        $r.Record('a') | Should -Be 0
        $r.RecordMany(@('b', 'c', 'd')) | Should -Be 3
        $r.Count() | Should -Be 4
        $null = $r.RecordMany((1..50 | ForEach-Object { "x$_" }))
        $r.Count() | Should -Be 4
        $r.TotalSeen() | Should -Be 54
        $r.Snapshot().Count | Should -Be 4
        $r.Reset()
        $r.TotalSeen() | Should -Be 0
        $r.Flush()
        $r.FlushAsync()
        $r.Dispose()
    }
}

Describe 'SubEtha.HandleTable' {
    It 'names values by handles that do not survive removal' {
        $t = New-SubEthaHandleTable -Path (Join-Path $script:dir 'handles') -Capacity 2
        $t.MaxValueBytes | Should -Be 44
        $h = $t.Insert('first')
        $t.Contains($h) | Should -BeTrue
        Get-SEText $t.Get($h) | Should -Be 'first'
        Get-SEText $t.Remove($h) | Should -Be 'first'
        $t.Contains($h) | Should -BeFalse
        $null -eq $t.Get($h) | Should -BeTrue
        $handles = $t.InsertMany(@('a', 'b', 'c'))
        $handles.Count | Should -Be 2
        $many = $t.GetMany(@($handles[0], $h))
        Get-SEText $many[0] | Should -Be 'a'
        $null -eq $many[1] | Should -BeTrue
        $t.Count() | Should -Be 2
        { $t.Insert('d') } | Should -Throw
        $t.Dispose()
    }
}

Describe 'SubEtha.TimePointTile' {
    It 'shows a reader only what existed at its version' {
        $t = New-SubEthaTimePointTile -Path (Join-Path $script:dir 'tile')
        $t.Lanes | Should -Be 16
        $l1 = $t.Insert(10, 'ten')
        $l2 = $t.Insert(20, 'twenty')
        $t.Count() | Should -Be 2
        $t.VisibleCount(15) | Should -Be 1
        $t.VisibleCount(25) | Should -Be 2
        ($t.VisibleMask(15) -band (1 -shl $l1)) | Should -Not -Be 0
        $seen = $t.Visible(15)
        $seen.Count | Should -Be 1
        Get-SEText $seen[0].Bytes | Should -Be 'ten'
        $seen[0].Version | Should -Be 10
        $at = $t.At($l2)
        $at.Version | Should -Be 20
        $t.Remove($l2)
        $null -eq $t.At($l2) | Should -BeTrue
        $t.IsFull() | Should -BeFalse
        { $t.At(99) } | Should -Throw
        $t.Dispose()
    }
}

Describe 'SubEtha.VersionChain' {
    It 'keeps every version and answers as of a version' {
        $c = New-SubEthaVersionChain -Path (Join-Path $script:dir 'chain') -Capacity 4
        $null -eq $c.Current() | Should -BeTrue
        $c.Push(1, 'one')
        $c.Push(5, 'five')
        $c.Count() | Should -Be 2
        Get-SEText $c.ReadAt(3) | Should -Be 'one'
        Get-SEText $c.ReadAt(9) | Should -Be 'five'
        $null -eq $c.ReadAt(0) | Should -BeTrue
        $cur = $c.Current()
        $cur.Version | Should -Be 5
        { $c.Push(2, 'late') } | Should -Throw
        $c.Clear()
        $c.Count() | Should -Be 0
        $c.Dispose()
    }
}

Describe 'SubEtha.VersionedSlab' {
    It 'keeps slot history and reads through a pin' {
        $slab = New-SubEthaVersionedSlab -Path (Join-Path $script:dir 'vslab') -Capacity 4 -EpochsPath (Join-Path $script:dir 'vslab-epochs')
        $slab.Depth | Should -Be 4
        $slab.Set(0, 'a')
        $pin = $slab.Pin()
        $pin.GetType().FullName | Should -Be 'SubEtha.SlabPin'
        $pin.Held() | Should -BeTrue
        $pin.Epoch() | Should -BeGreaterThan 0
        $slab.Set(0, 'b')
        Get-SEText $slab.Get(0) | Should -Be 'b'
        Get-SEText $pin.Get(0) | Should -Be 'a'
        $many = $pin.GetMany(@(0, 1))
        Get-SEText $many[0] | Should -Be 'a'
        $null -eq $many[1] | Should -BeTrue
        $history = $slab.History(0)
        $history.Count | Should -Be 2
        $null -eq $history[0].Died | Should -BeTrue
        $history[1].Died | Should -Not -BeNullOrEmpty
        $pin.Release()
        $pin.Held() | Should -BeFalse
        { $pin.Get(0) } | Should -Throw
        Get-SEText $slab.Retire(0) | Should -Be 'b'
        $null -eq $slab.Get(0) | Should -BeTrue
        $slab.SweepSlot(0) | Should -BeGreaterOrEqual 0
        $slab.VoidEpoch(999999) | Should -Be 0
        $slab.Flush()
        $slab.Dispose()
    }
}

Describe 'SubEtha.VersionedMap' {
    It 'scans one unchanging view through a pin' {
        $m = New-SubEthaVersionedMap -Path (Join-Path $script:dir 'vmap') -Capacity 64 -EpochsPath (Join-Path $script:dir 'vmap-epochs')
        $null -eq $m.Insert(1, 10) | Should -BeTrue
        $m.Insert(1, 11) | Should -Be 10
        $m.InsertMany(@([uint64] 2, [uint64] 3), @([uint64] 20, [uint64] 30)).Count | Should -Be 2
        $m.Get(2) | Should -Be 20
        $null -eq $m.Get(9) | Should -BeTrue
        $pin = $m.Pin()
        $m.Insert(4, 40) | Out-Null
        $m.Remove(1) | Should -Be 11
        $pin.Get(1) | Should -Be 11
        $null -eq $pin.Get(4) | Should -BeTrue
        $scan = $pin.Scan($null, $null, 100)
        ($scan | ForEach-Object { $_.Key }) | Should -Be @(1, 2, 3)
        $from = $pin.ScanFrom(2, $null, 100)
        ($from.Entries | ForEach-Object { $_.Value }) | Should -Be @(20, 30)
        $pin.Release()
        $m.Sweep() | Should -BeGreaterOrEqual 0
        $m.Count() | Should -BeGreaterOrEqual 3
        $m.Flush()
        $m.Dispose()
    }
}

Describe 'SubEtha.LanedMap' {
    It 'writes through lanes and reads across them' {
        $m = New-SubEthaLanedMap -Directory (Join-Path $script:dir 'laned') -Lanes 2
        $m.Lanes | Should -Be 2
        $claim = $m.ClaimLane()
        $claim.GetType().FullName | Should -Be 'SubEtha.LaneClaim'
        $claim.Held() | Should -BeTrue
        $null -eq $claim.Insert(7, 70) | Should -BeTrue
        $claim.InsertMany(@([uint64] 8), @([uint64] 80)).Count | Should -Be 1
        $m.HeldLanes() | Should -Be 1
        $m.Get(7) | Should -Be 70
        $m.LaneOf(7) | Should -Be $claim.Index()
        $other = $m.ClaimLane()
        { $other.Remove(7) } | Should -Throw
        $other.Release()
        $claim.Remove(8) | Should -Be 80
        $claim.Release()
        $m.HeldLanes() | Should -Be 0
        $again = $m.ClaimLaneFor(7)
        $again.Index() | Should -Be $m.LaneOf(7)
        $again.Release()
        { $m.ClaimLaneFor(12345) } | Should -Throw
        $pin = $m.Pin()
        ($pin.Scan($null, $null, 100) | ForEach-Object { $_.Key }) | Should -Be @(7)
        $pin.Get(7) | Should -Be 70
        $pin.ScanFrom($null, $null, 100).Entries.Count | Should -Be 1
        $pin.Release()
        $m.ReapDeadClaims() | Should -BeGreaterOrEqual 0
        $m.Sweep() | Should -BeGreaterOrEqual 0
        $m.Count() | Should -BeGreaterOrEqual 1
        $m.Flush()
        $m.Dispose()
    }
}

Describe 'SubEtha.TopologyMap' {
    It 'reads the shape off the traffic' {
        $t = New-SubEthaTopologyMap -Path (Join-Path $script:dir 'topology') -Participants 4
        $t.Participants | Should -Be 4
        $t.RecordSend(0, 1) | Should -Be 1
        $t.RecordMany(@([uint32] 0, [uint32] 0), @([uint32] 2, [uint32] 3)) | Should -Be 2
        $t.FanOut(0) | Should -Be 3
        $t.FanIn(1) | Should -Be 1
        $t.TotalSends() | Should -Be 3
        $busy = $t.BusiestSender()
        $busy.Participant | Should -Be 0
        $busy.Places | Should -Be 3
        $t.BusiestReceiver().Places | Should -Be 1
        $shape = $t.Recommend()
        $t.PublishRecommendation() | Should -Be $shape
        $t.PublishedRecommendation() | Should -Be $shape
        $t.RecommendationEpoch() | Should -BeGreaterThan 0
        $t.BroadcastRoot() | Should -BeGreaterOrEqual 0
        $t.Dispose()
    }
}

Describe 'SubEtha.Graph' {
    It 'adds nodes and edges and walks them' {
        $g = New-SubEthaGraph -Path (Join-Path $script:dir 'graph') -MaxNodes 8 -MaxEdges 8
        $a = $g.AddNode(100)
        $b = $g.AddNode(200)
        $more = $g.AddNodes(@([uint64] 300, [uint64] 400))
        $more.Count | Should -Be 2
        $e = $g.AddEdge($a, $b, 5)
        $g.AddEdges(@([uint32] $a), @([uint32] $more[0]), @([uint64] 6)).Count | Should -Be 1
        $g.NodeCount() | Should -Be 4
        $g.EdgeCount() | Should -Be 2
        $g.NodeValue($b) | Should -Be 200
        $null -eq $g.NodeValue(99) | Should -BeTrue
        $g.OutDegree($a) | Should -Be 2
        $n = $g.Neighbors($a)
        $n.Count | Should -Be 2
        ($n | Where-Object { $_.Edge -eq $e }).Value | Should -Be 5
        $g.RemoveEdge($a, $e) | Should -Be 5
        $g.OutDegree($a) | Should -Be 1
        $g.Flush()
        $g.FlushAsync()
        $g.Dispose()
    }
}

Describe 'SubEtha.Universal' {
    It 'stores itself as a list and moves to a map' {
        $u = New-SubEthaUniversal -Path (Join-Path $script:dir 'universal') -Capacity 64
        $u.Strategy() | Should -Be ([SubEtha.SetStrategy]::List)
        $u.Insert(1)
        $u.InsertMany(@([uint64] 2, [uint64] 3)) | Should -Be 2
        $u.Contains(2) | Should -BeTrue
        $u.ContainsMany(@([uint64] 3, [uint64] 9)) | Should -Be @($true, $false)
        $u.Count() | Should -Be 3
        ($u.Snapshot() | Sort-Object) | Should -Be @(1, 2, 3)
        $m = $u.Migrations()
        $u.MigrateTo([SubEtha.SetStrategy]::Map)
        $u.Strategy() | Should -Be ([SubEtha.SetStrategy]::Map)
        $u.Migrations() | Should -BeGreaterThan $m
        $u.Contains(1) | Should -BeTrue
        $counts = $u.OpCounts()
        $counts.Inserts | Should -BeGreaterOrEqual 3
        $u.Generation() | Should -BeGreaterOrEqual 0
        $u.Clear()
        $u.Count() | Should -Be 0
        $u.Dispose()
    }
}

Describe 'SubEtha.Tower' {
    It 'reaches values by a path that checks itself' {
        $t = New-SubEthaTower -Path (Join-Path $script:dir 'tower-bottom') -Capacity 8 -ValueSize 4 -LevelPath @((Join-Path $script:dir 'tower-top')) -LevelCapacity @(8)
        $t.Depth | Should -Be 2
        $t.ValueSize | Should -Be 4
        $path = $t.Append('abcd')
        $path.Count | Should -Be 2
        Get-SEText $t.Get($path) | Should -Be 'abcd'
        $paths = $t.AppendMany(@('efgh', 'ijkl'))
        $paths.Count | Should -Be 2
        $got = $t.GetMany($paths)
        Get-SEText $got[1] | Should -Be 'ijkl'
        $top = $t.InsertAtTop(5, 'mnop')
        $top[0] | Should -Be 5
        Get-SEText $t.Get($top) | Should -Be 'mnop'
        $t.Count() | Should -Be 4
        $wrong = [uint32[]] @($path[0], $path[1] + 1)
        { $t.Get($wrong) } | Should -Throw
        $t.Flush()
        $t.Dispose()
    }
}
