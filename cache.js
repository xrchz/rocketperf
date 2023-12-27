import 'dotenv/config'
import { ethers } from 'ethers'
import { db, provider, chainId, beaconRpcUrl, log, multicall, secondsPerSlot,
         timeSlotConvs, slotsPerEpoch, epochOfSlot, minipoolAbi,
         minipoolsByPubkeyCount, minipoolsByPubkey, minipoolCount,
         updateMinipoolCount, incrementMinipoolsByPubkeyCount, getIndexFromPubkey,
         getMinipoolByPubkey, getFinalizedSlot, getPubkeyFromIndex,
         rocketMinipoolManager, FAR_FUTURE_EPOCH
       } from './lib.js'
import { Worker } from 'node:worker_threads'
const {timeToSlot, slotToTime} = timeSlotConvs(chainId)

const MAX_QUERY_RANGE = 1000
const MAX_ARGS = 10000
const MAX_BEACON_RANGE = 100

const arrayMin = (a) => {
  let min = Infinity
  while (a.length) min = Math.min(min, ...a.splice(0, MAX_ARGS))
  return min
}

const arrayPromises = async (a, max, logger) => {
  log(`Processing ${a.length} promises ${max} at a time`)
  const result = []
  while (a.length) {
    logger(a.length)
    result.push(...await Promise.all(a.splice(0, max).map(f => f())))
  }
  return result
}

const rocketStorage = new ethers.Contract(
  await provider.resolveName('rocketstorage.eth'),
  ['event NodeWithdrawalAddressSet (address indexed node, address indexed withdrawalAddress, uint256 time)'],
  provider
)

const rocketStorageGenesisBlockByChain = {
  1: 13325233
}
const statusDissolved = 4n

const rocketStorageGenesisBlock = rocketStorageGenesisBlockByChain[chainId]

let withdrawalAddressBlock = db.get(`${chainId}/withdrawalAddressBlock`)
if (!withdrawalAddressBlock) withdrawalAddressBlock = rocketStorageGenesisBlock

async function updateWithdrawalAddresses() {
  const finalizedBlockNumber = await provider.getBlock('finalized').then(b => b.number)
  while (withdrawalAddressBlock < finalizedBlockNumber) {
    const min = withdrawalAddressBlock
    const max = Math.min(withdrawalAddressBlock + MAX_QUERY_RANGE, finalizedBlockNumber)
    log(`Processing withdrawal addresses ${min}...${max}`)
    const logs = await rocketStorage.queryFilter('NodeWithdrawalAddressSet', min, max)
    for (const entry of logs) {
      const nodeAddress = entry.args[0]
      const withdrawalAddress = entry.args[1]
      await db.transaction(() => {
        const key = `${chainId}/withdrawalAddress/${withdrawalAddress}`
        const nodeAddresses = db.get(key) || new Set()
        nodeAddresses.add(nodeAddress)
        db.put(key, nodeAddresses)
      })
    }
    withdrawalAddressBlock = max
    await db.put(`${chainId}/withdrawalAddressBlock`, withdrawalAddressBlock)
  }
}

async function updateMinipoolPubkeys() {
  const prevMinipoolCount = minipoolCount
  await updateMinipoolCount()
  while (minipoolsByPubkeyCount < minipoolCount) {
    const n = Math.min(MAX_QUERY_RANGE, minipoolCount - minipoolsByPubkeyCount)
    const minipoolAddresses = await multicall(
      Array(n).fill().map((_, i) => ({
        contract: rocketMinipoolManager,
        fn: 'getMinipoolAt',
        args: [minipoolsByPubkeyCount + i]
      }))
    )
    const pubkeys = await multicall(
      minipoolAddresses.map(minipoolAddress => ({
        contract: rocketMinipoolManager,
        fn: 'getMinipoolPubkey',
        args: [minipoolAddress]
      }))
    )
    for (const [i, minipoolAddress] of minipoolAddresses.entries()) {
      const pubkey = pubkeys[i]
      const currentEntry = minipoolsByPubkey.get(pubkey)
      if (currentEntry && currentEntry != minipoolAddress) {
        const currentMinipool = new ethers.Contract(currentEntry, minipoolAbi, provider)
        const pendingMinipool = new ethers.Contract(minipoolAddress, minipoolAbi, provider)
        const currentStatus = await currentMinipool.getStatus()
        const pendingStatus = await pendingMinipool.getStatus()
        if (currentStatus == pendingStatus)
          throw new Error(`Duplicate minipools with status ${currentStatus} for ${pubkey}: ${minipoolAddress} vs ${currentEntry}`)
        const activeAddress = currentStatus == statusDissolved ? minipoolAddress :
                              pendingStatus == statusDissolved ? currentEntry : null
        if (!activeAddress)
          throw new Error(`Duplicate minipools for ${pubkey} with neither dissolved: ${minipoolAddress} vs ${currentEntry}`)
        log(`Found duplicate minipools for ${pubkey}: ${minipoolAddress} vs ${currentEntry}. Keeping ${activeAddress}.`)
        minipoolsByPubkey.set(pubkey, activeAddress)
      }
      else {
        const currentMinipool = new ethers.Contract(minipoolAddress, minipoolAbi, provider)
        const currentStatus = await currentMinipool.getStatus()
        if (currentStatus == statusDissolved)
          log(`Ignoring minipool ${minipoolAddress} for ${pubkey} because it is dissolved`)
        else
          minipoolsByPubkey.set(pubkey, minipoolAddress)
      }
    }
    incrementMinipoolsByPubkeyCount(n)
    log(`Got pubkeys for ${minipoolsByPubkeyCount} minipools`)
  }
  if (minipoolCount != prevMinipoolCount) {
    await db.put(`${chainId}/minipoolsByPubkeyCount`, minipoolsByPubkeyCount)
    await db.put(`${chainId}/minipoolsByPubkey`, minipoolsByPubkey)
  }
}

provider.addListener('block', () => Promise.all([
  updateWithdrawalAddresses(),
  updateMinipoolPubkeys()
]))

async function getActivationInfo(validatorIndex) {
  const key = `${chainId}/validator/${validatorIndex}/activationInfo`
  const activationInfo = db.get(key) ?? {}
  let changed = false
  if (!('beacon' in activationInfo)) {
    const path = `/eth/v1/beacon/states/finalized/validators/${validatorIndex}`
    const url = new URL(path, beaconRpcUrl)
    const res = await fetch(url)
    const json = await res.json()
    const epoch = parseInt(json?.data?.validator?.activation_epoch)
    if (!(0 <= epoch))
      throw new Error(`Failed to get activation_epoch for ${validatorIndex}`)
    if (epoch == FAR_FUTURE_EPOCH)
      log(`Skipping setting unknown activation epoch for ${validatorIndex}`)
    else {
      activationInfo.beacon = epoch
      changed = true
    }
  }
  if (!('promoted' in activationInfo)) {
    const pubkey = await getPubkeyFromIndex(validatorIndex)
    const minipoolAddress = getMinipoolByPubkey(pubkey)
    const minipoolExists = await rocketMinipoolManager.getMinipoolExists(minipoolAddress)
    if (!minipoolExists)
      throw new Error(`Validator ${validatorIndex} (${pubkey}) has no corresponding minipool (${minipoolAddress})`)
    const minipool = new ethers.Contract(minipoolAddress, minipoolAbi, provider)
    if (await minipool.getVacant().catch(revert => false))
      throw new Error(`Minipool ${minipoolAddress} (validator ${validatorIndex}) is vacant`)
    const promotions = await minipool.queryFilter('MinipoolPromoted')
    if (promotions.length) {
      if (promotions.length !== 1)
        console.warn(`Unexpectedly many promotions for ${minipoolAddress}: ${promotions.length}`)
      const entry = promotions[0]
      const block = await entry.getBlock()
      const slot = timeToSlot(block.timestamp)
      log(`Got promotion @ block ${block.number} (${slot}) for ${minipoolAddress} (${validatorIndex})`)
      activationInfo.promoted = slot
    }
    else {
      log(`Recording ${minipoolAddress} (${validatorIndex}) as not a solo migration`)
      activationInfo.promoted = false
    }
    changed = true
  }
  if (changed) await db.put(key, activationInfo)
  return activationInfo
}

const epochFromActivationInfo = activationInfo =>
  activationInfo.promoted ?
    epochOfSlot(activationInfo.promoted) :
    activationInfo.beacon

const validatorStartEpochs = new Map()

const getValidatorsIdsForEpoch = (epoch) => new Set(
  Array.from(validatorStartEpochs.entries()).flatMap(
    ([id, actEp]) => actEp <= epoch ? [id] : []
  )
)

const rewardsOptionsForEpoch = (validatorIds) => ({
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify(Array.from(validatorIds.keys()))
})

let processedMinipoolCount = 0

const NUM_WORKERS = parseInt(process.env.NUM_WORKERS) || 2

const workers = Array.from(Array(NUM_WORKERS).keys()).map(
  i => ({
    worker: new Worker('./cacheEpochWorker.js'),
    promise: i,
    resolveWhenReady: null
  })
)

workers.forEach((data, i) => {
  data.worker.on('message', () => {
    if (typeof data.resolveWhenReady == 'function')
      data.resolveWhenReady(i)
  })
  data.worker.once('error', (e) => {
    console.error(`Error in worker ${worker.threadId}, exiting...`)
    console.error(e)
    process.exit(1)
  })
})

async function getWorker() {
  const i = await Promise.any(workers.map(({promise}) => promise))
  const data = workers[i]
  data.promise = new Promise(resolve => data.resolveWhenReady = resolve)
  return data.worker
}

let running = true

async function processEpochs() {
  const targetMinipoolCount = minipoolsByPubkey.size

  log(`targetMinipoolCount: ${targetMinipoolCount}, processedMinipoolCount: ${processedMinipoolCount}`)

  const validatorIdsToProcess = await arrayPromises(
    Array.from(minipoolsByPubkey.keys()).slice(
      processedMinipoolCount, targetMinipoolCount).map(
        pubkey => (() => getIndexFromPubkey(pubkey))
      ),
    MAX_BEACON_RANGE,
    (numLeft) => log(`Getting validatorIds, ${numLeft} left`)
  )

  processedMinipoolCount = targetMinipoolCount

  await arrayPromises(
    validatorIdsToProcess.map(validatorIndex =>
      async () => {
        if (!(0 <= parseInt(validatorIndex))) return
        const epoch = epochFromActivationInfo(await getActivationInfo(validatorIndex))
        if (typeof epoch == 'number')
          validatorStartEpochs.set(validatorIndex, epoch)
        else
          log(`Skipping ${validatorIndex} with start epoch ${epoch}`)
      }
    ),
    MAX_BEACON_RANGE,
    (numLeft) => log(`Getting activationInfo, ${numLeft} validators left`)
  )

  const finalizedSlot = await getFinalizedSlot()

  const startEpoch = arrayMin(
    Array.from(validatorStartEpochs.entries()).map(
      ([validatorIndex, activationEpoch]) =>
      Math.max(
        db.get(`${chainId}/validator/${validatorIndex}/nextEpoch`) ?? 0,
        activationEpoch
      )
    )
  )

  const finalEpoch = epochOfSlot(finalizedSlot - 1)

  log(`Getting data for epochs ${startEpoch} through ${finalEpoch}`)
  const epochsToProcess = Array.from(Array(finalEpoch - startEpoch + 1).keys()).map(x => startEpoch + x)
  const pendingEpochs = epochsToProcess.slice()

  while (running && epochsToProcess.length) {
    log(`${epochsToProcess.length} epochs left to process`)
    const epoch = epochsToProcess.shift()
    const validatorIds = getValidatorsIdsForEpoch(epoch)
    const worker = await getWorker()
    worker.prependOnceListener('message', async () => {
      const epochIndex = pendingEpochs.indexOf(epoch)
      pendingEpochs.splice(epochIndex, 1)
      if (epochIndex == 0) {
        const updated = []
        for (const validatorIndex of validatorIds.keys()) {
          const nextEpochKey = `${chainId}/validator/${validatorIndex}/nextEpoch`
          if ((db.get(nextEpochKey) || 0) < epoch) {
            updated.push(validatorIndex)
            await db.put(nextEpochKey, epoch)
          }
        }
        if (updated.length)
          log(`Updated nextEpoch to ${epoch} for ${updated}`)
      }
    })
    worker.postMessage({epoch, validatorIds})
  }
}

process.on('SIGINT', async () => {
  log(`Received interrupt...`)
  if (!running) {
    log(`Alreading shutting down...`)
    return
  }
  running = false
  log(`Removing listeners...`)
  await provider.removeAllListeners('block')
  log(`Terminating workers...`)
  await Promise.all(workers.map(({promise}) => promise))
  workers.forEach(({worker}) => worker.terminate())
  log(`Closing db...`)
  await db.close()
  process.exit()
})

while (true) {
  await processEpochs()
  await new Promise(resolve =>
    setTimeout(resolve, secondsPerSlot * slotsPerEpoch * 1000)
  )
}
