# p2p-local

Connections for the p2p layer on one machine: a socket in a folder, a request answered or a stream handed over.

One of the p2p series: the route between two processes of one machine.
The process that owns a folder serves a socket in it; whoever opens it is
a peer, authenticated by its keypair, so a peer is its public key as on
any other route. A connection is opened for a reason and ended with it:
none is kept.

## Links

- [test](test/)

## Usage

```js
import { socketOf, listen, request, open } from 'p2p-local'
const server = listen({ path: socketOf(dir, 'rns'), keyPair, onrequest, onstream })
const answer = await request(path, { op: 'renew' }, { keyPair, peer })   // peer: the key expected
const stream = await open(path, { keyPair, peer, hello })                // for replication
await server.close()
```

- `onrequest(body, { peer })`: what it returns is the answer; what it
  throws is thrown again where it was asked.
- `onstream(stream, { peer, hello })`: the connection itself, encrypted,
  for what both sides carry over it; ended by either side.
- `peer` given to `request` or `open`: the other side's public key, hex;
  another's, and nothing is sent to it: `not the peer expected`.
- No socket, or nobody serving it: `not reachable`.

## Design

- **The socket is the owner's**: only this machine's user opens it. What
  a peer may ask is the server's to decide, by its key.
- **Its path**: `<dir>/<name>.sock`; a path too long for a Unix socket: a
  short one in the temporary folder, by the folder's hash, the same for
  every process that asks.
- **A connection's first message says what it is**: `{ request }` or
  `{ stream }`; a request's answer `{ output }` or `{ error }`.
- **Another machine** is another route: the same keys, a connection over
  a network; not built here.

## Tests

- `brittle-bare test/*.test.js` (Bare installed): each test by its name in [test/](test/).
