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
         getRocketAddress, rocketMinipoolManager, minipoolAbi,
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

const rocketNodeManager = new ethers.Contract(
  await getRocketAddress('rocketNodeManager'), [
    'function getNodeExists(address) view returns (bool)',
    'function getNodeWithdrawalAddress(address) view returns (address)'
  ], provider)

log(`Node Manager: ${await rocketNodeManager.getAddress()}`)

const isNumber = /^[1-9]\d*$/

// TODO: cache entities per socketid?

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
    validatorIndex
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

function dateToDateTime(theDate) {
  const datestring = theDate.toISOString()
  const date = datestring.slice(0, 10)
  const time = datestring.slice(11, 11 + 8)
  return {date, time}
}

const emptyDutyData = { duties: 0, missed: 0, reward: 0n }
const emptyDay = () => ({
  attestations: {...emptyDutyData},
  proposals: {...emptyDutyData},
  syncs: {...emptyDutyData}
})
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

  socket.on('slotRangeLimits', async validatorIndices => {
    let max = await getFinalizedSlot()
    let min = max
    for (const validatorIndex of validatorIndices) {
      const activationInfo = db.get(`${chainId}/validator/${validatorIndex}/activationInfo`)
      if (!activationInfo) {
        console.warn(`No activationInfo for ${validatorIndex} trying to set slotRangeLimits`)
        continue
      }
      const validatorMin = activationInfo.promoted ?
        parseInt(activationInfo.promoted) :
        parseInt(activationInfo.beacon) * slotsPerEpoch
      min = Math.min(min, validatorMin)
      const nextEpoch = db.get(`${chainId}/validator/${validatorIndex}/nextEpoch`)
      if (typeof nextEpoch != 'number') {
        console.warn(`No nextEpoch for ${validatorIndex} trying to set slotRangeLimits`)
        max = Math.min(max, validatorMin)
      }
      else
        max = Math.min(max, nextEpoch * slotsPerEpoch)
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

  socket.on('perfDetails', async (fromSlot, toSlot, minipoolAddresses) => {
    const finalizedSlot = await getFinalizedSlot()
    if (!(0 <= fromSlot && fromSlot <= toSlot && toSlot <= finalizedSlot)) {
      console.warn(`Invalid slot range [${fromSlot}, ${toSlot}] (finalized ${finalizedSlot})`)
      return
    }
    const resultsBySlot = Array(toSlot - fromSlot + 1).fill().map(() => ({}))
    for (const minipoolAddress of minipoolAddresses) {
      const pubkey = await rocketMinipoolManager.getMinipoolPubkey(minipoolAddress)
      const validatorIndex = await getIndexFromPubkey(pubkey)
      const nextEpoch = db.get(`${chainId}/validator/${validatorIndex}/nextEpoch`)
      if (typeof(nextEpoch) != 'number' || nextEpoch < epochOfSlot(toSlot)) {
        console.warn(`Failed to include perfDetails for ${minipoolAddress}: epoch ${nextEpoch} no good for ${fromSlot}-${toSlot}`)
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
            attestations.reward += BigInt(attestation.reward[rewardType])
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
          syncs.duties += sync.rewards.length // may be < 32 if blocks were missed
          syncs.missed += sync.missed.length
          syncs.reward += sync.rewards.reduce((a, {reward}) => a + BigInt(reward), 0n)
          result.syncs = syncs
        }
      }
    }
    const date = new Date(slotToTime(fromSlot) * 1000)
    let currentDay = {...emptyDay()}
    let currentDayKey = date.getUTCDate()
    let currentMonth = {[currentDayKey]: currentDay}
    let currentMonthKey = date.getUTCMonth()
    let currentYear = {[currentMonthKey]: currentMonth}
    let currentYearKey = date.getUTCFullYear()
    const perfDetails = {[currentYearKey]: currentYear}
    for (const results of resultsBySlot.values()) {
      mergeIntoDay(currentDay, results)
      date.setMilliseconds(secondsPerSlot * 1000)
      if (currentDayKey !== date.getUTCDate()) {
        currentDay = {...emptyDay()}
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
