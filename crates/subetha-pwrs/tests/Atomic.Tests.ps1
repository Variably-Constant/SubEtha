# SubEtha.Atomic: a 64-bit integer in a file every process maps.
# PWRS_MODULE points at the built module folder.
BeforeAll {
    $module = $env:PWRS_MODULE
    if (-not $module) { throw 'PWRS_MODULE is not set' }
    Import-Module (Join-Path $module 'SubEtha.psd1') -Force -ErrorAction Stop
    $script:dir = Join-Path ([System.IO.Path]::GetTempPath()) ('subetha-ps-atomic-' + [guid]::NewGuid().ToString('n'))
    New-Item -ItemType Directory -Path $script:dir | Out-Null
}

AfterAll {
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
    Get-ChildItem -Path $script:dir -File | Remove-Item -Force -ErrorAction SilentlyContinue
    Remove-Item -Path $script:dir -Force -ErrorAction SilentlyContinue
}

Describe 'SubEtha.Atomic' {
    It 'creates the file holding Init and attaches to the live value' {
        $path = Join-Path $script:dir 'counter'
        $a = New-SubEthaAtomic -Path $path -Init 7
        $a.GetType().FullName | Should -Be 'SubEtha.Atomic'
        $a.Path | Should -Be $path
        $a.Load() | Should -Be 7
        $b = Open-SubEthaAtomic -Path $path
        $b.Load() | Should -Be 7
        $null = $a.FetchAdd(3)
        $b.Load() | Should -Be 10
        $a.Dispose()
        $b.Dispose()
    }

    It 'leaves the value alone when the file exists' {
        $path = Join-Path $script:dir 'existing'
        $a = New-SubEthaAtomic -Path $path -Init 5
        $b = New-SubEthaAtomic -Path $path -Init 99
        $b.Load() | Should -Be 5
        $a.Dispose()
        $b.Dispose()
    }

    It 'refuses to open a file that does not exist' {
        { Open-SubEthaAtomic -Path (Join-Path $script:dir 'missing') -ErrorAction Stop } | Should -Throw
    }

    It 'resolves a relative path against the current location' {
        Push-Location $script:dir
        try {
            $a = New-SubEthaAtomic -Path '.\relative' -Init 1
            $a.Path | Should -Be (Join-Path $script:dir 'relative')
            $a.Dispose()
        } finally {
            Pop-Location
        }
    }

    It 'returns the value before each read-modify-write' {
        $a = New-SubEthaAtomic -Path (Join-Path $script:dir 'rmw') -Init 10
        $a.FetchAdd() | Should -Be 10
        $a.FetchAdd(5) | Should -Be 11
        $a.FetchSub(1) | Should -Be 16
        $a.FetchOr(0xF0) | Should -Be 15
        $a.FetchAnd(0x0F) | Should -Be 255
        $a.FetchXor(0xFF) | Should -Be 15
        $a.Swap(42) | Should -Be 240
        $a.Load() | Should -Be 42
        $a.Load().GetType().Name | Should -Be 'UInt64'
        $a.Dispose()
    }

    It 'wraps round at the top and the bottom' {
        $a = New-SubEthaAtomic -Path (Join-Path $script:dir 'wrap') -Init ([uint64]::MaxValue)
        $null = $a.FetchAdd(1)
        $a.Load() | Should -Be 0
        $null = $a.FetchSub(1)
        $a.Load() | Should -Be ([uint64]::MaxValue)
        $a.Dispose()
    }

    It 'compares and exchanges, answering the value either way' {
        $a = New-SubEthaAtomic -Path (Join-Path $script:dir 'cas') -Init 1
        $a.CompareExchange(1, 2) | Should -Be 1
        $a.Load() | Should -Be 2
        $a.CompareExchange(1, 3) | Should -Be 2
        $a.Load() | Should -Be 2
        $a.Dispose()
    }

    It 'runs a batch of adds for one call' {
        $a = New-SubEthaAtomic -Path (Join-Path $script:dir 'batch') -Init 100
        $a.FetchAddMany(10) | Should -Be 100
        $a.Load() | Should -Be 110
        $a.FetchAddMany(3, 5) | Should -Be 110
        $a.Load() | Should -Be 125
        $a.Dispose()
    }

    It 'takes a memory order as the enum or as its name' {
        $a = New-SubEthaAtomic -Path (Join-Path $script:dir 'order') -Init 3
        $a.Load([SubEtha.MemoryOrder]::Relaxed) | Should -Be 3
        $a.Load('Acquire') | Should -Be 3
        $a.Store(4, 'Release')
        $a.Load() | Should -Be 4
        $a.FetchAdd(1, [SubEtha.MemoryOrder]::AcqRel) | Should -Be 4
        { $a.Load('Sideways') } | Should -Throw
        $a.Dispose()
    }

    It 'answers to the SE alias' {
        $path = Join-Path $script:dir 'alias'
        $a = New-SEAtomic -Path $path -Init 8
        $b = Open-SEAtomic -Path $path
        $b.Load() | Should -Be 8
        $a.Dispose()
        $b.Dispose()
    }

    It 'refuses a call after Dispose' {
        $a = New-SubEthaAtomic -Path (Join-Path $script:dir 'disposed')
        $a.Dispose()
        $a.IsDisposed | Should -BeTrue
        { $a.Load() } | Should -Throw
    }

    It 'lists the operations through Get-Member' {
        $a = New-SubEthaAtomic -Path (Join-Path $script:dir 'members')
        $names = ($a | Get-Member -MemberType Method).Name
        foreach ($n in 'Load', 'Store', 'FetchAdd', 'FetchSub', 'FetchOr', 'FetchAnd', 'FetchXor', 'Swap', 'CompareExchange', 'FetchAddMany', 'Dispose') {
            $names | Should -Contain $n
        }
        $a.Dispose()
    }
}
