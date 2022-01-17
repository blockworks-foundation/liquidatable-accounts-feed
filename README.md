# Liquidatable Accounts Feed

This implements a service that listens to Solana updates of Mango related accounts
and checks if they are liquidatable. If they are, it notifies connected clients
about it.

Its purpose is to hand potentially liquidatable accounts to the Mango liquidator.

It is a separate service because computing account health is around two orders of
magnitude faster this way.

## Architecture

Data flows into this service through
- Solana JSON RPC PubSub websocket streams, for slot and account updates
- Solana JSON RPC getProgramAccounts requests, for snapshots

The service models the current bank state for relevant accounts, checks their
health and sends interesting data back out to all clients that connected to its
websocket server.

All data resides in memory. The service does not write to disk.

## Configuration

Check `example-config.toml`.
