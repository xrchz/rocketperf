import 'dotenv/config'
import { ethers } from 'ethers'
import { readFileSync } from 'node:fs'
import express from 'express'
import helmet from 'helmet'
import https from 'node:https'
import { Server } from 'socket.io'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { db, log, provider, chainId, beaconRpcUrl,
         timeSlotConvs, getFinalizedSlot, epochOfSlot, secondsPerSlot }
       from './lib.js'

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

const {timeToSlot, slotToTime} = timeSlotConvs(chainId)

const nullAddress = '0x'.padEnd(42, '0')

const multicallContract = new ethers.Contract(
  '0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441',
  ['function aggregate((address,bytes)[]) view returns (uint256, bytes[])'],
  provider)
function multicall(calls) {
  log(`entered multicall with ${calls.length} calls`)
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
log(`Rocket Storage: ${await rocketStorage.getAddress()}`)

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
    'function getNodeExists(address) view returns (bool)',
    'function getNodeWithdrawalAddress(address) view returns (address)'
  ], provider)

log(`Minipool Manager: ${await rocketMinipoolManager.getAddress()}`)
log(`Node Manager: ${await rocketNodeManager.getAddress()}`)

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

// TODO: memoise? -- modulo withdrawalAddress and ens names (or store block)
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
    const validatorIndex = await getIndexFromPubkey(pubkey).then(i => (i < 0 ? 'none' : i.toString()))
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
    minipoolEnsName: await provider.lookupAddress(minipoolAddress),
    nodeAddress,
    nodeEnsName,
    withdrawalAddress,
    withdrawalEnsName,
    validatorIndex,
    selected: true
  }
}

// TODO: memoise? -- modulo withdrawalAddress and ens names (or store block)
async function lookupEntity(entity) {
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
      ).then(i => values.push(i))
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
      const nodeAddresses = db.get(`${chainId}/withdrawalAddress/${s}`)
      if (nodeAddresses) {
        for (const nodeAddress of nodeAddresses.values()) {
          if (starred || await rocketNodeManager.getNodeWithdrawalAddress(
                                 nodeAddress).then(w => w === s))
            await tryProcessNode(nodeAddress)
        }
      }
    }
    if (ethers.isHexString(s, 48)) {
      minipoolAddress = await rocketMinipoolManager.getMinipoolByPubkey(s)
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
        validatorInfo = {validatorIndex: s}
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
  log(`Returning ${values.length} values for ${entity}`)
  return {entity, values}
}

function setSlot(socket, dir, slot) {
  const keySlot = `${dir}Slot`
  const keyDate = `${dir}Date`
  const keyTime = `${dir}Time`
  const datestring = (new Date(slotToTime(slot) * 1000)).toISOString()
  const date = datestring.slice(0, 10)
  const time = datestring.slice(11, 11 + 8)
  socket.emit('setSlot', keySlot, slot)
  socket.emit('setSlot', keyDate, date)
  socket.emit('setSlot', keyTime, time)
}

const emptyDutyData = { duties: 0, missed: 0, reward: 0n }
const emptyDay = {
  attestations: {...emptyDutyData},
  proposals: {...emptyDutyData},
  syncs: {...emptyDutyData}
}
const mergeIntoDuty = (d, x) => Object.keys(d).forEach(k => d[k] += x[k])
const mergeIntoDay = (day, r) => Object.entries(day).forEach(([k, duty]) => {
  if (k in r) mergeIntoDuty(duty, r[k])
})

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

  socket.on('setSlot', async ({dir, type, value, other}) => {
    // TODO: maybe need to compare against previous value, to assess the intention of the change better
    const currentSlot = await getFinalizedSlot()
    let slot
    if (type === 'number') {
      slot = parseInt(value) || (dir === 'to' ? currentSlot : 0)
    }
    else {
      const datestring = type === 'date' ? `${value}T${other}` : `${other}T${value}`
      let time = (new Date(datestring)).getTime()
      time = time ? time / 1000 : slotToTime(dir === 'to' ? currentSlot : 0)
      slot = timeToSlot(time)
    }
    slot = Math.max(0, Math.min(slot, currentSlot))
    setSlot(socket, dir, slot)
  })

  socket.on('setSlotRange', async period => {
    const date = new Date()
    let fromSlot = 0
    const toSlot = await getFinalizedSlot()
    setSlot(socket, 'from', fromSlot)
    setSlot(socket, 'to', toSlot)
    if (period === 'year') {
      date.setMonth(0, 1)
      date.setHours(0, 0, 0, 0)
    }
    else if (period === 'month') {
      date.setDate(1)
      date.setHours(0, 0, 0, 0)
    }
    else if (period === 'week') {
      date.setDate(date.getDate() - date.getDay())
      date.setHours(0, 0, 0, 0)
    }
    else if (period === 'today') {
      date.setHours(0, 0, 0, 0)
    }
    else if (period === 'hour') {
      date.setMinutes(0, 0, 0)
    }
    fromSlot = timeToSlot(date.getTime() / 1000)
    if (fromSlot) setSlot(socket, 'from', fromSlot)
  })

  socket.on('perfDetails', async (fromSlot, toSlot, minipoolAddresses) => {
    // TODO: check slots are a valid range, i.e. in [0, finalizedSlot]
    const resultsBySlot = Array(toSlot - fromSlot).fill().map(() => {})
    for (const minipoolAddress of minipoolAddresses) {
      const pubkey = await rocketMinipoolManager.getMinipoolPubkey(minipoolAddress)
      const validatorIndex = await getIndexFromPubkey(pubkey)
      const nextEpoch = db.get(`${chainId}/validator/${validatorIndex}/nextEpoch`)
      if (typeof(nextEpoch) != 'number' || nextEpoch < epochOfSlot(toSlot)) {
        console.warn(`Failed to include perfDetails for ${minipoolAddress}: epoch ${nextEpoch} no good for ${fromSlot}-${toSlot}`)
        // TODO: update db instead?
        continue
      }
      // TODO: skip slots that are before this validator became active in rocket pool
      for (const [slotOffset, result] of resultsBySlot.entries()) {
        const slot = fromSlot + slotOffset
        const epoch = epochOfSlot(slot)
        const attestation = db.get(`${chainId}/validator/${validatorIndex}/attestation/${epoch}`)
        if (attestation?.slot === slot) {
          const attestations = result.attestations || {...emptyDutyData}
          attestations.duties += 1
          if (!attestation.attested)
            attestations.missed += 1
          for (const rewardType of ['head', 'target', 'source', 'inactivity'])
            attestations.reward += BigInt(attestation[rewardType])
          result.attestations = attestations
        }

        const proposal = db.get(`${chainId}/validator/${validatorIndex}/proposal/${slot}`)
        if (proposal) {
          const proposals = result.proposals || {...emptyDutyData}
          proposals.duties += 1
          if (proposal.missed)
            proposals.missed += 1
          proposals.reward += BigInt(proposal.reward)
          result.proposals = proposals
        }

        const sync = db.get(`${chainId}/validator/${validatorIndex}/sync/${epoch}`)
        if (sync) {
          const syncs = result.syncs || {...emptyDutyData}
          if (sync.rewards.length !== 32)
            console.warn(`unexpected sync.rewards.length ${sync.rewards.length} for ${validatorIndex} at ${epoch}`)
          syncs.duties += 32
          syncs.missed += sync.missed.length
          syncs.rewards += sync.rewards.reduce((a, s) => a + BigInt(s), 0n)
        }
      }
    }
    const date = new Date(slotToTime(fromSlot) * 1000)
    let currentDay = {...emptyDay}
    let currentDayKey = date.getUTCDate()
    let currentMonth = {[currentDayKey]: currentDay}
    let currentMonthKey = date.getUTCMonth()
    let currentYear = {[currentMonthKey]: currentMonth}
    let currentYearKey = date.getUTCFullYear()
    const perfDetails = {[currentYearKey]: currentYear}
    for (const results of resultsBySlot.entries()) {
      mergeIntoDay(currentDay, results)
      date.setMilliseconds(secondsPerSlot * 1000)
      if (currentDayKey !== date.getUTCDate()) {
        currentDay = {...emptyDay}
        currentDayKey = date.getUTCDate()
        if (currentMonthKey !== date.getUTCMonth()) {
          currentMonthKey = date.getUTCMonth()
          currentMonth = {}
          if (currentYearKey !== date.getUTCFullYear()) {
            currentYearKey = date.getUTCFullYear()
            currentYear = {}
            perfDetails[currentYearKey] = currentYear
          }
          currentYear[currentMonthKey] = currentMonth
        }
        currentMonth[currentDayKey] = currentDay
      }
    }
    Object.values(perfDetails).forEach(year =>
      Object.values(year).forEach(month =>
        Object.values(month).forEach(day =>
          Object.values(day).forEach(duty =>
            duty.reward = duty.reward.toString()))))
    socket.emit('perfDetails', perfDetails)
  })

  socket.on('disconnect', () => {
    log(`disconnection: ${socket.id}`)
  })

})
