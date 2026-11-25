// p2p-local: connections for the p2p layer on one machine: a socket in a
// folder, served by the process that owns the folder; whoever opens it is
// a peer, authenticated by its keypair (Noise), so a peer is its public
// key, as on any other route.
//   socketOf(dir, name)      where a folder's socket is: <dir>/<name>.sock,
//                            or a short path in the temporary folder, by
//                            the folder's hash, when that one is too long
//                            for a Unix socket
//   listen({ path, keyPair, onrequest, onstream })
//     onrequest(body, { peer })      a request answered: what it returns
//                                    (or its error) sent back, the
//                                    connection ended
//     onstream(stream, { peer, hello })   a stream handed over: the
//                                    connection itself, encrypted, for
//                                    what both sides carry over it
//                                    (replication); ended by either side
//     -> { path, close() }
//   request(path, body, { keyPair, peer, timeout })   -> the answer; throws
//                                    the error the other side answered
//                                    with, or `not reachable`
//   open(path, { keyPair, peer, hello })   -> a stream, as onstream gets it
//     peer      the public key (hex) the other side must have: another's,
//               and nothing is sent to it (`not the peer expected`)
// A connection is opened for a reason and ended with it: none is kept.
// The socket is the owner's (ownerOnly): this machine's user.
// A connection's first message says what it is: { request: body } or
// { stream: hello }; a request's answer: { output } or { error }.
import os from 'bare-os'
import crypto from 'bare-crypto'
import Pipe from 'bare-pipe'
import SecretStream from '@hyperswarm/secret-stream'
import { existsSync, rmSync } from 'bare-fs'
import { join } from 'bare-path'

const hex = b => (b ? Buffer.from(b).toString('hex') : null)

export function socketOf (dir, name) {
  const inside = join(dir, `${name}.sock`)
  if (Buffer.byteLength(inside) <= 100) return inside
  return join(os.tmpdir(), `p2p-${crypto.createHash('sha256').update(inside).digest('hex').slice(0, 16)}.sock`)
}

export function listen ({ path, keyPair, onrequest = null, onstream = null }) {
  // a socket left by a process that ended without closing: replaced
  rmSync(path, { force: true })
  const open = new Set()
  const server = Pipe.createServer(pipe => {
    pipe.on('error', () => {})
    const stream = new SecretStream(false, pipe, { keyPair })
    open.add(stream)
    stream.on('error', () => {})
    stream.on('close', () => open.delete(stream))
    stream.once('data', async first => {
      let message = null
      // catch: not a message of ours: the connection ended
      try { message = JSON.parse(first.toString()) } catch { return stream.destroy() }
      const peer = hex(stream.remotePublicKey)
      if (message && 'stream' in message && onstream) return onstream(stream, { peer, hello: message.stream })
      if (!message || !('request' in message) || !onrequest) return stream.destroy()
      let answer
      // catch: what the request's handler threw: answered as its error
      try { answer = { output: (await onrequest(message.request, { peer })) ?? null } } catch (err) { answer = { error: err.message } }
      stream.end(JSON.stringify(answer))
    })
  })
  server.listen({ path, ownerOnly: true })
  return {
    path,
    close () {
      for (const s of open) s.destroy()
      return new Promise(resolve => server.close(() => { rmSync(path, { force: true }); resolve() }))
    }
  }
}

const connect = (path, keyPair) => {
  if (!existsSync(path)) throw new Error(`not reachable: ${path} (no socket there)`)
  const pipe = new Pipe(path)
  const stream = new SecretStream(true, pipe, { keyPair })
  pipe.on('error', () => {})
  return stream
}

// The other side is the one expected: checked once the connection is
// open, before anything of ours is sent.
const expected = (stream, peer) => new Promise((resolve, reject) => {
  stream.once('error', reject)
  stream.once('close', () => reject(new Error('closed before it opened')))
  stream.once('open', () => {
    if (peer && hex(stream.remotePublicKey) !== peer) return reject(new Error(`not the peer expected: ${hex(stream.remotePublicKey).slice(0, 8)} (expected ${peer.slice(0, 8)})`))
    resolve()
  })
})

export function request (path, body, { keyPair, peer = null, timeout = 5000 } = {}) {
  return new Promise((resolve, reject) => {
    let stream
    // catch: no socket: said as not reachable
    try { stream = connect(path, keyPair) } catch (err) { return reject(err) }
    let done = false
    const finish = (err, value) => { if (done) return; done = true; clearTimeout(t); stream.destroy(); err ? reject(err) : resolve(value) }
    const t = setTimeout(() => finish(new Error(`no answer in ${timeout} ms: ${path}`)), timeout)
    stream.on('error', err => finish(new Error(`not reachable: ${path} (${err.code ?? err.message})`)))
    stream.on('close', () => finish(new Error(`not reachable: ${path} (closed before it answered)`)))
    stream.once('data', data => {
      let answer = null
      // catch: not an answer of ours
      try { answer = JSON.parse(data.toString()) } catch { return finish(new Error(`not an answer: ${path}`)) }
      if (answer && 'error' in answer) return finish(new Error(answer.error))
      finish(null, answer?.output ?? null)
    })
    expected(stream, peer).then(() => stream.write(JSON.stringify({ request: body })), err => finish(/^not the peer/.test(err.message) ? err : new Error(`not reachable: ${path} (${err.code ?? err.message})`)))
  })
}

export async function open (path, { keyPair, peer = null, hello = null } = {}) {
  const stream = connect(path, keyPair)
  stream.on('error', () => {})
  // catch: not opened, or another's: ended, said
  try { await expected(stream, peer) } catch (err) { stream.destroy(); throw /^not the peer/.test(err.message) ? err : new Error(`not reachable: ${path} (${err.code ?? err.message})`) }
  stream.write(JSON.stringify({ stream: hello }))
  return stream
}
