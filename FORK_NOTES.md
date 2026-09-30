# go-libp2p DCUtR validation fork

- Upstream: `github.com/libp2p/go-libp2p`
- Upstream version: `v0.48.0`
- Scope: `p2p/protocol/holepunch`, `p2p/transport/quicreuse`

This fork allows applications to validate an existing direct connection before
DCUtR treats it as `already connected`. A healthy direct connection is reused.
An unresponsive direct connection may be closed without closing the peer,
relay connection, host, or unrelated connections.

Relay triggers received while a hole-punch attempt is active are coalesced into
at most one bounded replay. Triggers received during that replay do not create
an unbounded retry chain.

## Validation

```sh
go test ./p2p/protocol/holepunch \
  -run '^(TestDirectConnectValidatesExistingDirectConnection|TestDirectConnectTriggerCoalescesOnePendingAttempt)$' \
  -count=20
go test -race ./p2p/protocol/holepunch \
  -run '^(TestDirectConnectValidatesExistingDirectConnection|TestDirectConnectTriggerCoalescesOnePendingAttempt)$' \
  -count=20
```

The default libp2p ping handler must remain enabled. Applications using this
fork should also keep an integration test that runs the validator while a
business stream is active and verifies that routing discovery still works
afterward.

## QUIC transport socket failure

Android destroys the sockets of a frozen background app. The QUIC Transport
then shuts down after a permanent read error, but upstream `quicreuse` keeps
the dead refcounted transport in its reuse maps until garbage collection
(`maxUnusedDuration` + `garbageCollectInterval`, 10 to 40 seconds). During
that window every QUIC dial selects the dead socket and fails, and listening
on the same port again fails with `address already in use`.

This fork closes such a transport as soon as its listener's Accept or a dial
reports a QUIC Transport closed with a non-`net.ErrClosed` cause, and drops it
from the reuse maps on the next lookup. A deliberately closed listener keeps
its transport until garbage collection as upstream does. The application still
has to call `Listen` again; the swarm does not recreate a failed listener.

```sh
go test ./p2p/transport/quicreuse   -run '^(TestSocketAbortIsNotTemporary|TestIsTransportFailure|TestDeadListeningTransportReleasesPortImmediately|TestDeliberateListenerCloseKeepsTransportForReuse|TestDeadDialingTransportIsReplaced)$'   -count=20
```
