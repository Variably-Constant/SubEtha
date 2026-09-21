# The module against its own surface: every exported cmdlet has its SE
# alias, a complete synopsis, and a test somewhere in this folder; every
# exported class and enum is named by a test. A name added to the module
# and not to the suites fails here.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:corpus = Get-ChildItem -Path $PSScriptRoot -Filter '*.Tests.ps1' |
        Where-Object { $_.Name -ne 'Surface.Tests.ps1' } |
        ForEach-Object { [System.IO.File]::ReadAllText($_.FullName) }
    $script:corpus = $script:corpus -join "`n"
    $script:cmdlets = @(Get-Command -Module SubEtha -CommandType Cmdlet)
    $script:aliases = @(Get-Command -Module SubEtha -CommandType Alias)
    # The shell assembly is loaded either under its plain name or with a
    # build hash appended, so that two module versions can sit in one
    # session without colliding. Both are this module's assembly.
    $script:shell = [AppDomain]::CurrentDomain.GetAssemblies() |
        Where-Object { $_.GetName().Name -eq 'SubEtha.Shell' -or $_.GetName().Name -like 'SubEtha.Shell.*' } |
        Select-Object -First 1
    if ($null -eq $script:shell) { throw 'the SubEtha.Shell assembly is not loaded; the module did not import' }
    $script:types = @($script:shell.GetExportedTypes() | Where-Object { $_.Namespace -eq 'SubEtha' })
}

Describe 'the cmdlets' {
    It 'number what the module declares' {
        $script:cmdlets.Count | Should -BeGreaterThan 130
        $script:aliases.Count | Should -Be $script:cmdlets.Count
    }

    It 'each answer to an SE alias that resolves to it' {
        foreach ($c in $script:cmdlets) {
            $short = $c.Name -replace 'SubEtha', 'SE'
            $alias = Get-Alias -Name $short -ErrorAction SilentlyContinue
            if ($null -eq $alias) { throw "$($c.Name) has no alias $short" }
            $alias.ResolvedCommand.Name | Should -Be $c.Name
        }
    }

    It 'each declare what they write, so nothing emits undeclared' {
        $missing = @($script:cmdlets | Where-Object { @($_.OutputType).Count -eq 0 } | ForEach-Object Name)
        $missing -join ', ' | Should -BeNullOrEmpty
    }

    It 'each carry an example in their help' {
        $missing = @($script:cmdlets | Where-Object {
            @((Get-Help $_.Name -Full).Examples.Example | Where-Object { $_ } |
                ForEach-Object { "$($_.Code)".Trim() } | Where-Object { $_ }).Count -eq 0
        } | ForEach-Object Name)
        $missing -join ', ' | Should -BeNullOrEmpty
    }

    It 'each carry a synopsis that is a whole sentence' {
        foreach ($c in $script:cmdlets) {
            $synopsis = (Get-Help $c.Name).Synopsis
            if ([string]::IsNullOrWhiteSpace($synopsis)) { throw "$($c.Name) has no synopsis" }
            if (-not $synopsis.Trim().EndsWith('.')) { throw "$($c.Name) synopsis is cut short: $synopsis" }
        }
    }

    It 'are each exercised by a suite' {
        $missing = @($script:cmdlets | Where-Object { $script:corpus -notmatch [regex]::Escape($_.Name) } | ForEach-Object Name)
        $missing -join ', ' | Should -BeNullOrEmpty
    }
}

Describe 'the classes and enums' {
    It 'are each exported from the shell' {
        $script:types.Count | Should -BeGreaterThan 120
    }

    It 'are each named by a suite' {
        $missing = @($script:types | Where-Object { $script:corpus -notmatch [regex]::Escape($_.FullName) } | ForEach-Object FullName)
        $missing -join ', ' | Should -BeNullOrEmpty
    }

    It 'each proxy is disposable and each enum has values' {
        foreach ($t in $script:types) {
            if ($t.IsEnum) {
                @([Enum]::GetValues($t)).Count | Should -BeGreaterThan 0
            } elseif ($t.BaseType -and $t.BaseType.Name -eq 'ProxyBase') {
                $t.GetMethod('Dispose') | Should -Not -BeNullOrEmpty
            }
        }
    }
}
