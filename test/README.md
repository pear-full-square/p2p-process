# test/

The tests of p2p-process, a section per file, each test by its name.

## `process.test.js`

- a request answered: the peer its public key, the answer back, the connection ended
- a request refused: the handler's error thrown here; no socket, a socket nobody serves: not reachable
- a stream handed over: both ways, encrypted, each side the other's key
- socketOf: in the folder; a path too long for a socket: a short one, the same each time
