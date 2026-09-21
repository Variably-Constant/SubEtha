---
title: "Bridge two hosts"
weight: 50
---

# Bridge two hosts

Everything else in the module shares memory on one machine. Three
transports reach another, and `Get-SubEthaTransport` lists the ones
built into the module you have:

```powershell
Get-SubEthaTransport          # sens tcp quic
```

They answer different questions. Pick by what you need, not by which is
newest.

| | Carries | Loss | Reach for it when |
|---|---|---|---|
| **TCP** | a whole ring | the stream handles it | both hosts are on a network you trust and you want the simplest thing |
| **QUIC** | a whole ring | the stream handles it | the link is public, or you want one connection to carry several streams |
| **Sens-O-Matic** | individual items | the reader rebuilds what was lost | items matter more than order, and asking again is too slow |

TCP and QUIC carry the ring: what one side sends into it, the other
side reads out. Sens-O-Matic is a different shape and is covered last.

## TCP

The server side attaches the ring and waits for one connection:

```powershell
$server = New-SubEthaTcpBridgeServer -RingPath C:\ipc\events -Capacity 4096 -LocalPort 9100
$server.LocalAddr()          # confirms the port, useful when LocalPort was 0
$server.AcceptOne()          # blocks until a client connects
```

The client side attaches its own ring and runs the exchange:

```powershell
$client = New-SubEthaTcpBridgeClient -RingPath C:\ipc\events -Capacity 4096 `
                                     -ServerHost 10.0.0.5 -ServerPort 9100
$client.Run(1000)            # carries up to 1000 items
```

`AcceptOne` blocks, so in one session it goes in another runspace
beside the client. Objects are safe to share across runspaces, which is
how the binding's own bridge tests are written:

```powershell
$job = Start-ThreadJob -ScriptBlock { param($s) $s.AcceptOne() } -ArgumentList $server
$client.Run(1000)
Receive-Job $job -Wait
```

Both ends name a capacity, and it has to agree with the ring already on
disk, because capacity is part of the layout rather than a hint.

## QUIC

The same shape with certificates. Generate a self-signed pair for a
test or an internal link:

```powershell
$cert = New-SubEthaSelfSignedCert -Name 'subetha-bridge'
```

The server takes the certificate and its key; the client takes the
certificate and the name to verify against:

```powershell
$server = New-SubEthaQuicBridgeServer -RingPath C:\ipc\events -Capacity 4096 `
                                      -LocalPort 9200 -Cert $cert.Cert -Key $cert.Key

$client = New-SubEthaQuicBridgeClient -RingPath C:\ipc\events -Capacity 4096 `
                                      -ServerHost 10.0.0.5 -ServerPort 9200 `
                                      -Cert $cert.Cert -ServerName 'subetha-bridge'
```

`-ServerName` must match the name the certificate was made for, not the
host you dialed. That is the usual first failure: a certificate made
for `subetha-bridge` and a `-ServerName` of `10.0.0.5` will not verify.

For anything outside a network you control, use a certificate from your
own authority rather than a self-signed one.

## Opening the port

Both bridges listen, so the listening host needs the port open. This is
the step that most often looks like a bridge fault and is not.

```powershell
# Windows, as administrator
New-NetFirewallRule -DisplayName 'SubEtha bridge' -Direction Inbound `
                    -Protocol TCP -LocalPort 9100 -Action Allow
```

```bash
# Linux, ufw
sudo ufw allow 9100/tcp
```

QUIC is UDP, not TCP: a rule allowing TCP on the port does nothing for
it. Use `-Protocol UDP` and `9200/udp` for the QUIC examples above.

## Sens-O-Matic, for a lossy link

The other two carry a ring over a stream that hides loss. Sens-O-Matic
sends more than the items so the reader can rebuild what the network
dropped without asking for it again, which suits a link where a
round trip costs more than the redundancy does.

```powershell
$reader = New-SubEthaSensReceiver -LocalPort 9000 -MaxItemSize 1024
$writer = New-SubEthaSensSender -PeerHost 10.0.0.5 -PeerPort 9000 -MaxItemSize 1024

$writer.SendMany(@('one', 'two', 'three'))

foreach ($item in $reader.Poll()) { [Text.Encoding]::UTF8.GetString($item) }
```

**Items do not arrive one at a time.** `Poll` answers whatever the link
could rebuild this time round, which may be nothing at all. A reader
calls it in a loop rather than expecting one item per call:

```powershell
while ($true) {
    foreach ($item in $reader.Poll()) { ... }
    Start-Sleep -Milliseconds 10
}
```

`MaxItemSize` is fixed when the endpoint is made and both ends must
agree on it.

Both ends report what the link is doing, which is the point of the
transport: `Loss()`, `DatagramsSent()` and `DatagramsReceived()` on the
sender, `SendFailures()` and `Alive()` on the receiver, and `Code()`
and `Switches()` on either.

## Where to go next

- [Bridge two hosts from Rust](../../cross-host-bridge/) for the same
  transports with the certificate and firewall detail in full, and the
  measured comparison between them.
- [Choose a structure](../choose-a-structure/) for what to put in the
  ring before bridging it.
- [The reference](../../../reference/subetha-pwrs/#the-bridges) for
  every method on the bridge and sensing objects.
