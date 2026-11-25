import test from 'brittle'
import { mkdtempSync, existsSync } from 'bare-fs'
import { join } from 'bare-path'
import { tmpdir } from 'bare-os'
import crypto from 'hypercore-crypto'
import { socketOf, listen, request, open } from '../index.js'

const dir = () => mkdtempSync(join(tmpdir(), 'p2p-process-'))
const hex = b => b.toString('hex')

test('a request answered: the peer its public key, the answer back, the connection ended', async (t) => {
  const d = dir()
  const a = crypto.keyPair()
  const b = crypto.keyPair()
  const seen = []
  const server = listen({ path: socketOf(d, 'x'), keyPair: a, onrequest: (body, { peer }) => { seen.push([body, peer]); return { got: body.n } } })
  t.ok(existsSync(server.path), 'its socket in the folder')
  t.alike(await request(server.path, { n: 7 }, { keyPair: b }), { got: 7 }, 'what the handler returned')
  t.alike(seen, [[{ n: 7 }, hex(b.publicKey)]], 'the peer: the public key it connected with')
  await t.exception(request(server.path, { n: 8 }, { keyPair: b, peer: hex(b.publicKey) }), /not the peer expected/, 'a peer expected by its key: another\'s refused')
  t.is(seen.length, 1, 'and nothing sent to it')
  await server.close()
  t.absent(existsSync(server.path), 'closed: its socket gone')
})

test('a request refused: the handler\'s error thrown here; no socket, a socket nobody serves: not reachable', async (t) => {
  const d = dir()
  const server = listen({ path: socketOf(d, 'x'), keyPair: crypto.keyPair(), onrequest: () => { throw new Error('not registered: swarm-1/x') } })
  await t.exception(request(server.path, {}, { keyPair: crypto.keyPair() }), /not registered: swarm-1\/x/)
  await server.close()
  await t.exception(request(server.path, {}, { keyPair: crypto.keyPair() }), /not reachable/)
  await t.exception(open(server.path, { keyPair: crypto.keyPair() }), /not reachable/)
})

test('a stream handed over: both ways, encrypted, each side the other\'s key', async (t) => {
  const d = dir()
  const a = crypto.keyPair()
  const b = crypto.keyPair()
  const got = new Promise(resolve => {
    const server = listen({ path: socketOf(d, 'x'), keyPair: a, onstream: (stream, { peer, hello }) => { stream.once('data', m => { stream.write(Buffer.from(`re: ${m}`)); resolve({ peer, hello, server }) }) } })
  })
  const stream = await open(socketOf(d, 'x'), { keyPair: b, peer: hex(a.publicKey), hello: { topic: 'log' } })
  const back = new Promise(resolve => stream.once('data', m => resolve(m.toString())))
  stream.write(Buffer.from('ping'))
  t.is(await back, 're: ping', 'carried both ways')
  const { peer, hello, server } = await got
  t.alike([peer, hello], [hex(b.publicKey), { topic: 'log' }], 'the peer and what it said it opens it for')
  t.is(hex(stream.remotePublicKey), hex(a.publicKey), 'the one who opened it sees the server\'s key')
  stream.destroy()
  await server.close()
})

test('socketOf: in the folder; a path too long for a socket: a short one, the same each time', (t) => {
  t.is(socketOf('/tmp/a', 'rns'), '/tmp/a/rns.sock')
  const long = `/tmp/${'x'.repeat(120)}`
  t.ok(Buffer.byteLength(socketOf(long, 'rns')) <= 100)
  t.is(socketOf(long, 'rns'), socketOf(long, 'rns'))
  t.not(socketOf(long, 'rns'), socketOf(long, 'log'))
})
