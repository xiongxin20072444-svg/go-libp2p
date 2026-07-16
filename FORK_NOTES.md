# go-libp2p DCUtR validation fork

- Upstream: `github.com/libp2p/go-libp2p`
- Upstream version: `v0.48.0`
- Scope: `p2p/protocol/holepunch`

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
