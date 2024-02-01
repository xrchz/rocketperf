import 'dotenv/config'
import { ethers } from 'ethers'
import { readFileSync } from 'node:fs'
import express from 'express'
import helmet from 'helmet'
import https from 'node:https'
import { Server } from 'socket.io'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { db, log, provider, chainId, beaconRpcUrl, nullAddress, slotsPerEpoch,
         multicall, getIndexFromPubkey, getPubkeyFromIndex, getMinipoolByPubkey,
         getRocketAddress, rocketMinipoolManager, minipoolAbi, epochFromActivationInfo,
         timeSlotConvs, getFinalizedSlot, epochOfSlot, secondsPerSlot }
       from './lib.js'

const app = express()

app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      'connect-src': ["'self'", 'wss:']
    }
  }
}))

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

const {timeToSlot, slotToTime} = timeSlotConvs(chainId)

const rocketNodeManager = new ethers.Contract(
  await getRocketAddress('rocketNodeManager'), [
    'function getNodeExists(address) view returns (bool)',
    'function getNodeWithdrawalAddress(address) view returns (address)'
  ], provider)

log(`Node Manager: ${await rocketNodeManager.getAddress()}`)

const isNumber = /^[1-9]\d*$/

const ENTITY_CACHE_MAX_SIZE = 8192
const entityCache = new Map()
const validatorCache = new Map()
// TODO: watch withdrawal address change events, and ENS name setting events (ReverseClaimed on the reverse registrar?) to invalidate cache entries
// TODO: cache separately things that change and things that don't:
//  entity -> index list -- update on withdrawal change events for withdrawal address entities, and ENS name events for ENS names, and minipool creations for node addresses
//  index -> minipool info without ENS names
//  address -> ENS name ? or just use provider lookup each time? (otherwise update on ENS name events)
//  OR just skip caching and make loading of this data more concurrent? i.e. return what is ready when it's ready (with volatile)

async function lookupMinipool({minipoolAddress, nodeInfo, withdrawalInfo, validatorInfo}) {
  log(`Lookup minipool ${minipoolAddress}`)
  const minipool = new ethers.Contract(minipoolAddress, minipoolAbi, provider)
  async function getNodeInfo() {
    const nodeAddress = await minipool.getNodeAddress()
    const nodeEnsName = await provider.lookupAddress(nodeAddress)
    return {nodeAddress, nodeEnsName}
  }
  async function getValidatorInfo() {
    const pubkey = await rocketMinipoolManager.getMinipoolPubkey(minipoolAddress)
    const validatorIndex = await getIndexFromPubkey(pubkey).then(i => (i < 0 ? 'none' : i))
    return {pubkey, validatorIndex}
  }
  async function getWithdrawalInfo(nodeAddress) {
    const withdrawalAddress = await rocketNodeManager.getNodeWithdrawalAddress(nodeAddress)
    const withdrawalEnsName = await provider.lookupAddress(withdrawalAddress)
    return {withdrawalAddress, withdrawalEnsName}
  }
  const {nodeAddress, nodeEnsName} = nodeInfo || await getNodeInfo()
  const {pubkey, validatorIndex} = validatorInfo || await getValidatorInfo()
  const {withdrawalAddress, withdrawalEnsName} = withdrawalInfo || await getWithdrawalInfo(nodeAddress)
  return {
    minipoolAddress,
    nodeAddress,
    nodeEnsName,
    withdrawalAddress,
    withdrawalEnsName,
    validatorIndex
  }
}

async function lookupEntity(entity) {
  const cached = entityCache.get(entity)
  if (cached) return {entity, values: cached}
  let s = entity
  let minipoolAddress, nodeInfo, withdrawalInfo, validatorInfo, starred = false
  const values = []
  async function tryProcessNode(s) {
    if (await rocketNodeManager.getNodeExists(s)) {
      log(`${s} exists as node`)
      const nodeAddress = s
      const nodeEnsName = await provider.lookupAddress(nodeAddress)
      nodeInfo = {nodeAddress, nodeEnsName}
      const withdrawalAddress = await rocketNodeManager.getNodeWithdrawalAddress(nodeAddress)
      const withdrawalEnsName = await provider.lookupAddress(withdrawalAddress)
      withdrawalInfo = {withdrawalAddress, withdrawalEnsName}
      const n = await rocketMinipoolManager.getNodeMinipoolCount(nodeAddress)
      log(`${s} has ${n} minipools`)
      const minipoolAddresses = await multicall(
        Array(parseInt(n)).fill().map((_, i) => ({
          contract: rocketMinipoolManager,
          fn: 'getNodeMinipoolAt',
          args: [nodeAddress, i]
        })))
      values.push(...await Promise.all(
        minipoolAddresses.map(minipoolAddress =>
          lookupMinipool({minipoolAddress, nodeInfo, withdrawalInfo})))
      )
      nodeInfo = null
      withdrawalInfo = null
    }
  }
  while (true) {
    if (!starred && s.endsWith('*')) {
      s = s.slice(0, -1)
      starred = true
    }
    if (minipoolAddress) {
      await lookupMinipool(
        {minipoolAddress, nodeInfo, validatorInfo}
      ).then(x => values.push(x))
      break
    }
    if (ethers.isAddress(s)) {
      log(`Parsed ${s} as an address`)
      if (await rocketMinipoolManager.getMinipoolExists(s)) {
        log(`${s} exists as minipool`)
        minipoolAddress = s
        continue
      }
      await tryProcessNode(s)
      const nodeAddresses = db.get([chainId,'withdrawalAddress',s])
      if (nodeAddresses) {
        for (const nodeAddress of nodeAddresses) {
          if (starred || await rocketNodeManager.getNodeWithdrawalAddress(
                                 nodeAddress).then(w => w === s))
            await tryProcessNode(nodeAddress)
        }
      }
    }
    if (ethers.isHexString(s, 48)) {
      minipoolAddress = getMinipoolByPubkey(s)
      if (minipoolAddress != nullAddress) {
        log(`${s} exists as minipool pubkey`)
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
      log(`${s} is a number`)
      const pubkey = await getPubkeyFromIndex(s)
      if (pubkey) {
        validatorInfo = {validatorIndex: parseInt(s)}
        s = pubkey
        continue
      }
    }
    log(`Trying ${s} as ENS`)
    try { s = await provider.resolveName(s) }
    catch { s = null }
    if (!s) break
    log(`resolved as ${s}: rerunning`)
  }
  log(`Caching and returning ${values.length} values for ${entity}`)
  entityCache.set(entity, values)
  while (entityCache.size > ENTITY_CACHE_MAX_SIZE)
    entityCache.delete(entityCache.keys().next().value)
  return {entity, values}
}

function dateToDateTime(theDate) {
  const datestring = theDate.toISOString()
  const date = datestring.slice(0, 10)
  const time = datestring.slice(11, 11 + 8)
  return {date, time}
}

const emptyDutyData = () => ({ duties: 0, missed: 0, reward: 0n, slots: new Set() })
const emptyDay = () => ({
  attestations: {...emptyDutyData()},
  proposals: {...emptyDutyData()},
  syncs: {...emptyDutyData()},
  slots: {min: Infinity, max: 0}
})
const mergeIntoDuty = (d, x) => Object.keys(d).forEach(k =>
  k == 'slots' ? x[k].forEach(v => d[k].add(v))
  : d[k] += x[k]
)
const mergeIntoDay = (day, r, slot) => Object.entries(day).forEach(([k, duty]) => {
  if (k == 'slots') {
    duty.min = Math.min(duty.min, slot)
    duty.max = Math.max(duty.max, slot)
  }
  else if (k in r) mergeIntoDuty(duty, r[k])
})

const setToRanges = (s) => {
  const a = Array.from(s)
  a.sort((x, y) => x - y)
  const r = []
  const fixLast = (l) => {
    if (l && l.max - l.min < 2)
      r.splice(-1, 1,
        ...(l.max === l.min ? [l.min] : [l.min, l.max])
      )
  }
  while (a.length) {
    const n = a.shift()
    const l = r.at(-1)
    if (n === l?.max + 1)
      l.max = n
    else {
      fixLast(l)
      r.push({min: n, max: n})
    }
  }
  fixLast(r.at(-1))
  return r
}

io.on('connection', socket => {

  log(`connection: ${socket.id}`)

  socket.on('entities', entities => {
    log(`Got entities from ${socket.id}: ${entities}`)
    Promise.all(
      entities
      .split(/\s|,/)
      .filter(s => s.length)
      .map(lookupEntity)
    ).then(minipools => {
        const uniqueMinipools = []
        const addresses = new Set()
        minipools.flatMap(x => x.values).forEach(x => {
          if (!addresses.has(x.minipoolAddress))
            uniqueMinipools.push(x)
          addresses.add(x.minipoolAddress)
        })
        socket.emit('minipools', uniqueMinipools)
        socket.emit('unknownEntities', minipools.flatMap(x => x.values.length ? [] : [x.entity]))
      }
    )
  })

  socket.on('slotRangeLimits', async validatorIndices => {
    let max = await getFinalizedSlot()
    let min = max
    for (const validatorIndex of validatorIndices) {
      const activationInfo = db.get([chainId,'validator',validatorIndex,'activationInfo'])
      if (!activationInfo) {
        console.warn(`No activationInfo for ${validatorIndex} trying to set slotRangeLimits`)
        continue
      }
      const validatorMin = activationInfo.promoted ?
        parseInt(activationInfo.promoted) :
        parseInt(activationInfo.beacon) * slotsPerEpoch
      min = Math.min(min, validatorMin)
      const nextEpoch = db.get([chainId,'validator',validatorIndex,'nextEpoch'])
      if (typeof nextEpoch != 'number') {
        console.warn(`No nextEpoch for ${validatorIndex} trying to set slotRangeLimits`)
        max = Math.min(max, validatorMin)
      }
      else
        max = Math.min(max, (nextEpoch - 1) * slotsPerEpoch)
    }
    socket.emit('slotRangeLimits', {min, max})
  })

  socket.on('timeToSlot', (time, callback) => {
    const slot = timeToSlot(time)
    callback(slot)
  })

  socket.on('slotToTime', (slot, callback) => {
    const date = new Date(slotToTime(slot) * 1000)
    callback(dateToDateTime(date))
  })

  socket.on('slotToTimestamp', (slot, callback) =>
    callback(slotToTime(slot))
  )

  socket.on('validatorPerformance', async (validatorIndex, fromSlot, toSlot, callback) => {
    // log(`${socket.id} requesting validatorPerformance ${validatorIndex} ${fromSlot} ${toSlot}`)
    const result = {}
    const activationEpoch = epochFromActivationInfo(db.get([chainId,'validator',validatorIndex,'activationInfo']))
    const nextEpoch = db.get([chainId,'validator',validatorIndex,'nextEpoch'])
    if (typeof(nextEpoch) != 'number' || nextEpoch <= epochOfSlot(toSlot))
      return callback({error: {nextEpoch}})
    if (typeof(activationEpoch) != 'number' || epochOfSlot(toSlot) < activationEpoch)
      return callback({error: {activationEpoch}})
    const day = {...emptyDay()}
    day.slots.min = fromSlot
    day.slots.max = toSlot
    let slot = Math.max(fromSlot, activationEpoch * slotsPerEpoch)
    while (slot <= toSlot) {
      // log(`Up to slot ${slot} out of ${toSlot} for ${validatorIndex} for ${socket.id}`)
      const epoch = epochOfSlot(slot)

      const attestation = db.get([chainId,'validator',validatorIndex,'attestation',epoch])
      if (attestation?.slot === slot) {
        const attestations = day.attestations
        attestations.duties += 1
        if (!attestation.attested)
          attestations.missed += 1
        for (const rewardType of ['head', 'target', 'source', 'inactivity']) {
          if (typeof attestation.reward[rewardType] != 'string' && rewardType != 'inactivity')
            throw new Error(`Missing ${rewardType} reward in ${JSON.stringify(attestation)}`)
          attestations.reward += BigInt(attestation.reward[rewardType] || 0)
        }
        attestations.slots.add(slot)
      }

      const proposal = db.get([chainId,'validator',validatorIndex,'proposal',parseInt(slot)])
      if (proposal) {
        const proposals = day.proposals
        proposals.duties += 1
        if (proposal.missed)
          proposals.missed += 1
        proposals.reward += BigInt(proposal.reward)
        proposals.slots.add(slot)
      }

      const sync = db.get([chainId,'validator',validatorIndex,'sync',epoch])
      const syncReward = sync?.rewards.find(({slot: syncSlot}) => slot == syncSlot)
      const syncMissed = sync?.missed.includes(slot)
      if (syncReward || syncMissed) {
        const syncs = day.syncs
        syncs.duties += 1
        syncs.missed += syncMissed
        syncs.reward += BigInt(syncReward.reward)
        syncs.slots.add(slot)
      }

      slot++
    }
    Object.entries(day).forEach(([key, duty]) => {
      if (key == 'slots') return
      duty.reward = duty.reward.toString()
      duty.slots = setToRanges(duty.slots)
    })
    return callback(day)
  })

  socket.on('disconnect', () => {
    log(`disconnection: ${socket.id}`)
  })

})
