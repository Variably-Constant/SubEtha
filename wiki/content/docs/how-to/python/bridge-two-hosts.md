---
title: "Bridge two hosts"
weight: 50
---

# Bridge two hosts

Everything else in the package shares memory on one machine. Three
transports reach another, and a wheel carries only the ones it was
built with:

```python
>>> subetha.transports
['sens', 'tcp', 'quic']
```

Ask that rather than assuming. `subetha.OPTIONAL_BY_TRANSPORT` says
what an absent transport would have brought, which is how you tell a
wheel built without one from a name that never existed.

| | Carries | Loss | Reach for it when |
|---|---|---|---|
| **TCP** | a whole ring | the stream handles it | both hosts are on a network you trust and you want the simplest thing |
| **QUIC** | a whole ring | the stream handles it | the link is public, or one connection should carry several streams |
| **Sens-O-Matic** | individual items | the reader rebuilds what was lost | items matter more than order, and asking again is too slow |

## The bridges take a ring, not a path

This is the shape to notice. A bridge is constructed from a `Ring`
object you already have, so the ring is opened once and the bridge
carries it:

```python
ring = subetha.Ring("/ipc/events", capacity=4096)

server = subetha.TcpBridgeServer(ring, ("0.0.0.0", 9100))
server.local_addr           # confirms the port, useful when it was 0
server.accept_one()         # blocks until a client connects, answers a count
```

The client side opens its own ring and runs the exchange:

```python
ring = subetha.Ring("/ipc/events", capacity=4096)
client = subetha.TcpBridgeClient(ring, ("10.0.0.5", 9100))
client.run(1000)            # carries up to 1000 items
```

An address is a `(host, port)` tuple throughout, and `local_addr`
answers one.

`accept_one` blocks, so in one program it goes on another thread beside
the client. The calls release the interpreter, so a thread really does
wait rather than holding everything up:

```python
import threading

t = threading.Thread(target=server.accept_one)
t.start()
client.run(1000)
t.join()
```

## QUIC

The same shape with certificates. `generate_self_signed_cert` answers
the certificate and the key as two `bytes`, for a test or an internal
link:

```python
cert, key = subetha.generate_self_signed_cert("subetha-bridge")

server = subetha.QuicBridgeServer(ring, ("0.0.0.0", 9200), cert, key)
client = subetha.QuicBridgeClient(ring, ("10.0.0.5", 9200), cert, "subetha-bridge")
```

The name the client verifies against must match the name the
certificate was made for, not the host it dialed. That is the usual
first failure: a certificate made for `subetha-bridge` and a server
name of `10.0.0.5` will not verify.

For anything outside a network you control, use a certificate from your
own authority rather than a self-signed one.

## Opening the port

Both bridges listen, so the listening host needs the port open. This is
the step that most often looks like a bridge fault and is not.

```bash
sudo ufw allow 9100/tcp            # Linux, TCP bridge
sudo ufw allow 9200/udp            # Linux, QUIC bridge
```

```powershell
New-NetFirewallRule -DisplayName 'SubEtha bridge' -Direction Inbound `
                    -Protocol TCP -LocalPort 9100 -Action Allow
```

QUIC is UDP, not TCP: a rule allowing TCP on the port does nothing for
it.

## Sens-O-Matic, for a lossy link

The other two carry a ring over a stream that hides loss. Sens-O-Matic
sends more than the items so the reader can rebuild what the network
dropped without asking again, which suits a link where a round trip
costs more than the redundancy does.

```python
reader = subetha.SensReceiver(("0.0.0.0", 9000), max_item_size=1024)
writer = subetha.SensSender(("0.0.0.0", 0), ("10.0.0.5", 9000), max_item_size=1024)

writer.send_many([b"one", b"two", b"three"])

for item in reader.poll():
    ...
```

**Items do not arrive one at a time.** `poll` answers whatever the link
could rebuild this time round, which may be an empty list. A reader
calls it in a loop rather than expecting one item per call, and
`poll_from` answers each item with the position it belongs at.

From a coroutine, `aio.poll` does the waiting without blocking the
loop. It asks on the calling thread and yields between tries, because a
`SensReceiver` cannot leave its thread.

`max_item_size` is fixed when the endpoint is made and both ends must
agree on it.

## Where to go next

- [Bridge two hosts from Rust](../../cross-host-bridge/) for the same
  transports with the certificate and firewall detail in full, and the
  measured comparison between them.
- [Threads, asyncio and lifetimes](../threads-and-lifetimes/) for
  running `accept_one` beside other work.
- [Every class in full](../../../reference/subetha-py/classes/) for
  every method on the bridge and sensing classes.
