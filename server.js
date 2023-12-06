import 'dotenv/config'
import { ethers } from 'ethers'
import { readFileSync } from 'node:fs'
import express from 'express'
import helmet from 'helmet'
import https from 'node:https'
import { Server } from 'socket.io'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const app = express()

app.use(helmet())

// when obtaining the certificate:
// app.use('/.well-known', express.static('.well-known'))
const httpsOptions = {
  key: readFileSync(process.env.KEY),
  cert: readFileSync(process.env.CERT)
}

const options = {root: dirname(fileURLToPath(import.meta.url))}

app.get('/', (req, res) => {
  res.sendFile('index.html', options)
})

app.get('/index.js', (req, res) => {
  res.sendFile('index.js', options)
})

app.get('/index.css', (req, res) => {
  res.sendFile('index.css', options)
})

app.get('/icon.png', (req, res) => {
  res.sendFile('icon.png', options)
})

const server = https.createServer(httpsOptions, app)
const io = new Server(server)
server.listen(443)

const provider = new ethers.JsonRpcProvider(process.env.RPC)
const beaconRpcUrl = process.env.BN

const nullAddress = '0x'.padEnd(42, '0')

const multicallContract = new ethers.Contract(
  '0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441',
  ['function aggregate((address,bytes)[]) view returns (uint256, bytes[])'],
  provider)
function multicall(calls) {
  console.log(`entered multicall with ${calls.length} calls`)
  const margs = []
  const posts = []
  for (const {contract, fn, args} of calls) {
    const iface = contract.interface
    const ff = iface.getFunction(fn)
    const data = iface.encodeFunctionData(ff, args)
    margs.push(contract.getAddress().then(addr => [addr, data]))
    posts.push(res => iface.decodeFunctionResult(ff, res)[0])
  }
  return Promise.all(margs)
    .then(calls => multicallContract.aggregate(calls))
    .then(res => Array.from(res[1]).map((r, i) => posts[i](r)))
}

const rocketStorage = new ethers.Contract(
  await provider.resolveName('rocketstorage.eth'),
  ['function getAddress(bytes32 _key) view returns (address)'],
  provider
)
console.log(`Rocket Storage: ${await rocketStorage.getAddress()}`)

const getRocketAddress = name => rocketStorage['getAddress(bytes32)'](ethers.id(`contract.address${name}`))

const rocketMinipoolManager = new ethers.Contract(
  await getRocketAddress('rocketMinipoolManager'), [
    'function getMinipoolExists(address) view returns (bool)',
    'function getMinipoolByPubkey(bytes) view returns (address)',
    'function getMinipoolPubkey(address) view returns (bytes)',
    'function getNodeMinipoolCount(address) view returns (uint256)',
    'function getNodeMinipoolAt(address, uint256) view returns (address)'
  ], provider)

const rocketNodeManager = new ethers.Contract(
  await getRocketAddress('rocketNodeManager'), [
    'function getNodeExists(address) view returns (bool)'
  ], provider)

console.log(`Minipool Manager: ${await rocketMinipoolManager.getAddress()}`)
console.log(`Node Manager: ${await rocketNodeManager.getAddress()}`)

const minipoolAbi = [
  'function getNodeAddress() view returns (address)'
]

async function getIndexFromPubkey(pubkey) {
  const path = `/eth/v1/beacon/states/head/validators/${pubkey}`
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status === 404)
    return -1
  if (response.status !== 200)
    console.warn(`Unexpected response status getting ${pubkey} index: ${response.status}`)
  return await response.json().then(j => j.data.index)
}

async function getPubkeyFromIndex(index) {
  const path = `/eth/v1/beacon/states/head/validators/${index}`
  const url = new URL(path, beaconRpcUrl)
  const response = await fetch(url)
  if (response.status === 404)
    return null
  if (response.status !== 200)
    console.warn(`Unexpected response status getting ${index} pubkey: ${response.status}`)
  return await response.json().then(j => j.data.validator.pubkey)
}

const isNumber = /^[1-9]\d*$/

// TODO: cache entities, including selectedness (per socketid?)

async function lookupMinipool({minipoolAddress, nodeInfo, validatorInfo}) {
  console.log(`Lookup minipool ${minipoolAddress}`)
  const minipool = new ethers.Contract(minipoolAddress, minipoolAbi, provider)
  async function getNodeInfo() {
    const nodeAddress = await minipool.getNodeAddress()
    const nodeEnsName = await provider.lookupAddress(nodeAddress)
    return {nodeAddress, nodeEnsName}
  }
  async function getValidatorInfo() {
    const pubkey = await rocketMinipoolManager.getMinipoolPubkey(minipoolAddress)
    const validatorIndex = await getIndexFromPubkey(pubkey).then(i => (i < 0 ? 'none' : i.toString()))
    return {pubkey, validatorIndex}
  }
  const {nodeAddress, nodeEnsName} = nodeInfo || await getNodeInfo()
  const {pubkey, validatorIndex} = validatorInfo || await getValidatorInfo()
  return {
    minipoolAddress,
    minipoolEnsName: await provider.lookupAddress(minipoolAddress),
    nodeAddress,
    nodeEnsName,
    validatorIndex,
    selected: true
  }
}

async function lookupEntity(entity) {
  let s = entity
  let minipoolAddress
  let nodeInfo
  let validatorInfo
  const items = []
  while (true) {
    if (minipoolAddress) {
      await lookupMinipool(
        {minipoolAddress, nodeInfo, validatorInfo}
      ).then(i => items.push(i))
      break
    }
    if (ethers.isAddress(s)) {
      console.log(`Parsed ${s} as an address`)
      if (await rocketMinipoolManager.getMinipoolExists(s)) {
        console.log(`${s} exists as minipool`)
        minipoolAddress = s
        continue
      }
      if (await rocketNodeManager.getNodeExists(s)) {
        console.log(`${s} exists as node`)
        const nodeAddress = s
        const nodeEnsName = await provider.lookupAddress(nodeAddress)
        nodeInfo = {nodeAddress, nodeEnsName}
        const n = await rocketMinipoolManager.getNodeMinipoolCount(nodeAddress)
        console.log(`${s} has ${n} minipools`)
        const minipoolAddresses = await multicall(
          Array(parseInt(n)).fill().map((_, i) => ({
            contract: rocketMinipoolManager,
            fn: 'getNodeMinipoolAt',
            args: [nodeAddress, i]
          })))
        items.push(...await Promise.all(
          minipoolAddresses.map(minipoolAddress =>
            lookupMinipool({minipoolAddress, nodeInfo})))
        )
        nodeInfo = null
      }
      // TODO: check withdrawal address
    }
    if (ethers.isHexString(s, 48)) {
      minipoolAddress = await rocketMinipoolManager.getMinipoolByPubkey(s)
      if (minipoolAddress != nullAddress) {
        console.log(`${s} exists as minipool pubkey`)
        const pubkey = s
        const validatorIndex = validatorInfo?.validatorIndex || await getIndexFromPubkey(pubkey)
        if (0 <= validatorIndex) {
          validatorInfo = {pubkey, validatorIndex}
          continue
        }
      }
      else {
        minipoolAddress = null
      }
    }
    if (isNumber.test(s)) {
      console.log(`${s} is a number`)
      const pubkey = await getPubkeyFromIndex(s)
      if (pubkey) {
        validatorInfo = {validatorIndex: s}
        s = pubkey
        continue
      }
    }
    console.log(`${s} not found, trying as ENS`)
    try { s = await provider.resolveName(s) }
    catch { s = null }
    if (!s) break
    console.log(`resolved as ${s}: rerunning`)
  }
  console.log(`Returning ${items.length} items for ${entity}`)
  return items
  // TODO: inform about entities that failed to resolve
}

io.on('connection', socket => {

  console.log(`connection: ${socket.id}`)

  socket.on('entities', entities => {
    console.log(`Got entities from ${socket.id}: ${entities}`)
    Promise.all(
      entities
      .split(/\s|,/)
      .filter(s => s.length)
      .map(lookupEntity)
    ).then(minipools =>
      socket.emit('minipools', minipools.flat())
    )
  })

  socket.on('disconnect', () => {
    console.log(`disconnection: ${socket.id}`)
  })

})
