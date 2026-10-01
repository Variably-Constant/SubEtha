# The shm: drive: values under names, shared by every process of the
# user and reached as $shm:name. The suite registers a drive of its own
# over files in a scratch directory, so it never touches the user's.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'shm'
    $script:exe = [System.Diagnostics.Process]::GetCurrentProcess().MainModule.FileName
    $script:manifest = Join-Path $env:PWRS_MODULE 'SubEtha.psd1'
    $null = New-PSDrive -Name shmt -PSProvider SubEthaShm -Root $script:dir -Scope Global

    # Runs $Body in a fresh process of the host running this suite, with
    # the module imported and a drive over the same files registered
    # first, and answers its output lines as one array.
    function Invoke-SEChild {
        param([string] $Body)
        $file = Join-Path $script:dir ('child-' + [guid]::NewGuid().ToString('n') + '.ps1')
        $text = "`$ErrorActionPreference = 'Stop'`r`nImport-Module '$($script:manifest)'`r`n`$null = New-PSDrive -Name shmt -PSProvider SubEthaShm -Root '$($script:dir)'`r`n$Body"
        [System.IO.File]::WriteAllText($file, $text)
        $out = @(& $script:exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $file 2>&1 | ForEach-Object { "$_" })
        if ($LASTEXITCODE -ne 0) { throw "the child process failed: $($out -join [Environment]::NewLine)" }
        Write-Output -NoEnumerate $out
    }
}

AfterAll {
    Remove-PSDrive -Name shmt -Force -ErrorAction SilentlyContinue
    Remove-SubEthaScratch $script:dir
}

Describe 'the shm: drive' {
    It 'is registered when the module imports' {
        (Get-PSDrive -Name shm).Provider.Name | Should -Be 'SubEthaShm'
    }

    It 'reads a missing name as null' {
        $shmt:missing | Should -BeNullOrEmpty
        Test-Path 'shmt:missing' | Should -BeFalse
    }

    It 'gives an assigned value back as its own type' {
        $shmt:count = 5
        $shmt:count | Should -Be 5
        $shmt:count.GetType().Name | Should -Be 'Int32'
        $shmt:big = 5000000000
        $shmt:big.GetType().Name | Should -Be 'Int64'
        $shmt:ratio = 2.5
        $shmt:ratio | Should -Be 2.5
        $shmt:ratio.GetType().Name | Should -Be 'Double'
        $shmt:flag = $true
        $shmt:flag | Should -BeTrue
        $shmt:flag.GetType().Name | Should -Be 'Boolean'
        $shmt:word = 'hello'
        $shmt:word | Should -Be 'hello'
        $shmt:word.GetType().Name | Should -Be 'String'
    }

    It 'keeps a DateTime to the tick, with its kind' {
        $now = [datetime]::Now
        $shmt:when = $now
        $shmt:when | Should -Be $now
        $shmt:when.Kind | Should -Be $now.Kind
        $utc = [datetime]::UtcNow
        $shmt:whenUtc = $utc
        $shmt:whenUtc.Ticks | Should -Be $utc.Ticks
        $shmt:whenUtc.Kind | Should -Be 'Utc'
    }

    It 'keeps a byte array as bytes' {
        $shmt:bytes = [byte[]] (1, 2, 3, 250)
        ($shmt:bytes -join ',') | Should -Be '1,2,3,250'
        $shmt:bytes.GetType().Name | Should -Be 'Byte[]'
    }

    It 'gives an object back as the property bag remoting would' {
        $shmt:record = [pscustomobject] @{ Name = 'x'; Size = 12 }
        $shmt:record.Name | Should -Be 'x'
        $shmt:record.Size | Should -Be 12
        $shmt:table = @{ key = 'value'; count = 3 }
        $shmt:table['key'] | Should -Be 'value'
        $shmt:table['count'] | Should -Be 3
        $shmt:list = 1, 2, 3
        ($shmt:list -join ',') | Should -Be '1,2,3'
    }

    It 'matches names without regard to case and lists them as first written' {
        $shmt:Greeting = 'hi'
        $shmt:GREETING | Should -Be 'hi'
        $shmt:greeting = 'hello'
        $shmt:Greeting | Should -Be 'hello'
        (Get-ChildItem 'shmt:' | Where-Object Name -eq 'Greeting').Value | Should -Be 'hello'
        (Get-ChildItem 'shmt:' | Where-Object { $_.Name -ceq 'GREETING' }) | Should -BeNullOrEmpty
    }

    It 'removes a name assigned null, or removed as an item' {
        $shmt:gone = 1
        $shmt:gone = $null
        Test-Path 'shmt:gone' | Should -BeFalse
        $shmt:item = 2
        Remove-Item 'shmt:item'
        $shmt:item | Should -BeNullOrEmpty
        { Remove-Item 'shmt:item' -ErrorAction Stop } | Should -Throw
    }

    It 'serves Set-Item and Get-Item' {
        Set-Item 'shmt:si' -Value 42
        (Get-Item 'shmt:si').Value | Should -Be 42
        (Get-Item 'shmt:si').Name | Should -Be 'si'
        $shmt:si | Should -Be 42
    }

    It 'shares a value with another process, which can replace it' {
        $shmt:counter = 5
        $out = Invoke-SEChild 'Write-Output $shmt:counter; $shmt:counter = 6; Write-Output $shmt:counter'
        [int] $out[-2] | Should -Be 5
        [int] $out[-1] | Should -Be 6
        $shmt:counter | Should -Be 6
    }

    It 'keeps a value after the process that wrote it has exited' {
        $null = Invoke-SEChild '$shmt:legacy = ''kept''; $shmt:legacyObject = [pscustomobject] @{ From = ''child'' }'
        $shmt:legacy | Should -Be 'kept'
        $shmt:legacyObject.From | Should -Be 'child'
    }
}
