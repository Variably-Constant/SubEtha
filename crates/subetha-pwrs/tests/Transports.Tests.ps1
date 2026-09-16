# The bridges over loopback, and the list of transports.
BeforeAll {
    . (Join-Path $PSScriptRoot 'Common.ps1')
    Import-SubEthaModule
    $script:dir = New-SubEthaScratch 'transports'

    # Runs $Script in another runspace with $Argument, and answers a
    # handle whose Result() waits for what the script returned. The
    # bridges' Run and AcceptOne each block until the other end
    # finishes, so one of them has to run away from the pipeline thread.
    function Start-SEBackground {
        param([scriptblock] $Script, [object] $Argument)
        $ps = [powershell]::Create()
        $null = $ps.AddScript($Script.ToString()).AddArgument($Argument)
        $handle = $ps.BeginInvoke()
        [pscustomobject] @{ Shell = $ps; Handle = $handle }
    }

    function Wait-SEBackground {
        param($Job)
        $out = $Job.Shell.EndInvoke($Job.Handle)
        if ($Job.Shell.HadErrors) { throw ($Job.Shell.Streams.Error | Out-String) }
        $Job.Shell.Dispose()
        $out
    }
}

AfterAll {
    Remove-SubEthaScratch $script:dir
}

Describe 'Get-SubEthaTransport' {
    It 'lists the transports carried' {
        Get-SubEthaTransport | Should -Be @('sens', 'tcp', 'quic')
    }
}

Describe 'SubEtha.TcpBridge' {
    It 'ships a ring to another ring over loopback' {
        $source = Join-Path $script:dir 'tcp-source'
        $sink = Join-Path $script:dir 'tcp-sink'
        $from = New-SubEthaRing -Path $source -Capacity 64
        $to = New-SubEthaRing -Path $sink -Capacity 64
        $p = $from.RegisterProducer()
        $c = $to.RegisterConsumer()
        1..5 | ForEach-Object { $null = $from.Send($p, "item-$_") }

        $server = New-SubEthaTcpBridgeServer -RingPath $sink -Capacity 64 -LocalPort 0 -LocalHost 127.0.0.1
        $server.GetType().FullName | Should -Be 'SubEtha.TcpBridgeServer'
        $addr = $server.LocalAddr()
        $addr.Port | Should -BeGreaterThan 0
        $accepting = Start-SEBackground -Script { param($s) $s.AcceptOne() } -Argument $server

        $client = New-SubEthaTcpBridgeClient -RingPath $source -Capacity 64 -ServerHost 127.0.0.1 -ServerPort $addr.Port
        $client.Server.Port | Should -Be $addr.Port
        $client.Run(5)
        $arrived = Wait-SEBackground $accepting
        $arrived | Should -Be 5
        ($to.RecvMany($c, 10) | ForEach-Object { ConvertFrom-SEBytes $_ }) -join ',' | Should -Be 'item-1,item-2,item-3,item-4,item-5'
        $client.Dispose()
        $server.Dispose()
        $from.Dispose()
        $to.Dispose()
    }
}

Describe 'SubEtha.QuicBridge' {
    It 'ships a ring to another ring over loopback with a self-signed certificate' {
        $cert = New-SubEthaSelfSignedCert -Name 'localhost'
        $cert.GetType().FullName | Should -Be 'SubEtha.Certificate'
        $cert.Name | Should -Be 'localhost'
        $cert.Cert.Length | Should -BeGreaterThan 0
        $cert.Key.Length | Should -BeGreaterThan 0

        $source = Join-Path $script:dir 'quic-source'
        $sink = Join-Path $script:dir 'quic-sink'
        $from = New-SubEthaRing -Path $source -Capacity 64
        $to = New-SubEthaRing -Path $sink -Capacity 64
        $p = $from.RegisterProducer()
        $c = $to.RegisterConsumer()
        1..3 | ForEach-Object { $null = $from.Send($p, "q-$_") }

        $server = New-SubEthaQuicBridgeServer -RingPath $sink -Capacity 64 -LocalPort 0 -LocalHost 127.0.0.1 -Cert $cert.Cert -Key $cert.Key
        $addr = $server.LocalAddr()
        $addr.Port | Should -BeGreaterThan 0
        $accepting = Start-SEBackground -Script { param($s) $s.AcceptOne() } -Argument $server

        $client = New-SubEthaQuicBridgeClient -RingPath $source -Capacity 64 -ServerHost 127.0.0.1 -ServerPort $addr.Port -Cert $cert.Cert -ServerName 'localhost'
        $client.ServerName | Should -Be 'localhost'
        $client.Run(3)
        Wait-SEBackground $accepting | Should -Be 3
        ($to.RecvMany($c, 10) | ForEach-Object { ConvertFrom-SEBytes $_ }) -join ',' | Should -Be 'q-1,q-2,q-3'
        $client.Dispose()
        $server.Dispose()
        $from.Dispose()
        $to.Dispose()
    }

    It 'refuses a certificate it cannot read' {
        { New-SubEthaQuicBridgeClient -RingPath (Join-Path $script:dir 'none') -Capacity 8 -ServerHost 127.0.0.1 -ServerPort 1 -Cert ([byte[]](1, 2, 3)) -ServerName x -ErrorAction Stop } | Should -Throw
    }
}
