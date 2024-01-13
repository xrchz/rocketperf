import 'dotenv/config'
import { fetch, setGlobalDispatcher, Agent } from 'undici'
import { ethers } from 'ethers'
import { db, provider, chainId, beaconRpcUrl, log, multicall, secondsPerSlot,
         timeSlotConvs, slotsPerEpoch, epochOfSlot, minipoolAbi, arrayMin, arrayMax,
         finishMoreTasks, arrayPromises, interruptArrayPromises, filterResolved,
         minipoolsByPubkeyCount, minipoolsByPubkey, minipoolCount,
         updateMinipoolCount, incrementMinipoolsByPubkeyCount, getIndexFromPubkey,
         getMinipoolByPubkey, getFinalizedSlot, getPubkeyFromIndex,
         rocketMinipoolManager, FAR_FUTURE_EPOCH, epochFromActivationInfo
       } from './lib.js'
const {timeToSlot, slotToTime} = timeSlotConvs(chainId)

setGlobalDispatcher(new Agent({ connect: { timeout: 60e3 } }) )

const MAX_QUERY_RANGE = 1000
const MAX_BEACON_RANGE = 100

const NUM_EPOCH_TASKS = parseInt(process.env.NUM_EPOCH_TASKS) || 16
const NUM_INDEX_TASKS = parseInt(process.env.NUM_INDEX_TASKS) || 2048

const OVERRIDE_START_EPOCH = parseInt(process.env.OVERRIDE_START_EPOCH)
const STANDARD_START_EPOCH = isNaN(OVERRIDE_START_EPOCH)
const DUTIES_ONLY = !!process.env.DUTIES_ONLY
if (DUTIES_ONLY && !STANDARD_START_EPOCH)
  throw new Error('DUTIES_ONLY cannot use OVERRIDE_START_EPOCH')

const OVERRIDE_FINAL_SLOT = parseInt(process.env.OVERRIDE_FINAL_SLOT)
const STANDARD_FINAL_SLOT = isNaN(OVERRIDE_FINAL_SLOT)

function hexStringToBitvector(s) {
  const bitlist = []
  let hexDigits = s.substring(2)
  if (hexDigits.length % 2 !== 0)
    hexDigits = `0${hexDigits}`
  let i
  while (hexDigits.length) {
    const byteStr = hexDigits.substring(0, 2)
    hexDigits = hexDigits.substring(2)
    const uint8 = parseInt(`0x${byteStr}`)
    i = 1
    while (i < 256) {
      bitlist.push(!!(uint8 & i))
      i *= 2
    }
  }
  return bitlist
}

function hexStringToBitlist(s) {
  const bitlist = hexStringToBitvector(s)
  let i = bitlist.length
  while (!bitlist[--i])
    bitlist.pop()
  bitlist.pop()
  return bitlist
}

let running = true

const rocketStorage = new ethers.Contract(
  await provider.resolveName('rocketstorage.eth'),
  ['event NodeWithdrawalAddressSet (address indexed node, address indexed withdrawalAddress, uint256 time)'],
  provider
)

const rocketStorageGenesisBlockByChain = {
  1: 13325233
}
const statusStaking = 2n
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

let blockLock
if (STANDARD_START_EPOCH && !DUTIES_ONLY) {
  provider.addListener('block', async () => {
    if (!blockLock) {
      blockLock = Promise.all([
        updateWithdrawalAddresses(),
        updateMinipoolPubkeys()
      ])
      await blockLock
      blockLock = false
    }
  })
}

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
    if (await minipool.getStatus().then(s => s == statusStaking)) {
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
    else
      log(`Skipping setting solo/promotion for ${validatorIndex} whose minipool is not yet staking`)
  }
  if (changed) await db.put(key, activationInfo)
  return activationInfo
}

const validatorActivationEpochs = new Map()

const getValidatorsIdsForEpoch = (validatorNextEpochs, epoch) => new Set(
  validatorNextEpochs.flatMap(
    ([id, nextEpoch]) => nextEpoch <= epoch ? [id] : []
  )
)

const rewardsOptionsForEpoch = (validatorIds) => ({
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify(Array.from(validatorIds.keys()))
})

let processedMinipoolCount = 0

async function getAttestationDuties(epoch, validatorIds) {
  log(`Getting attestation duties for ${epoch}`)

  if (!validatorIds.size) {
    console.warn(`${epoch} has no relevant active validators`)
    return
  }

  const firstSlotInEpoch = epoch * slotsPerEpoch

  const attestationDutiesUrl = new URL(
    `/eth/v1/beacon/states/${firstSlotInEpoch}/committees?epoch=${epoch}`,
    beaconRpcUrl
  )

  if (!running) return

  const logAddedSet = new Set()
  const logAddedList = []
  const logAdded = (index, slot) => {
    logAddedSet.add(index)
    logAddedList.push(slot)
  }

  const committees = await fetch(attestationDutiesUrl).then(async res => {
    if (res.status !== 200)
      throw new Error(`Got ${res.status} fetching attestation duties for ${epoch}: ${await res.text()}`)
    const json = await res.json()
    return json.data
  })
  for (const {index, slot, validators} of committees) {
    if (!running) break
    for (const [position, selectedIndex] of validators.entries()) {
      if (!running) break
      if (validatorIds.has(selectedIndex)) {
        const attestationKey = `${chainId}/validator/${selectedIndex}/attestation/${epoch}`
        const attestation = db.get(attestationKey) || {}
        if (!('position' in attestation)) {
          // log(`Adding attestation duty @ ${slot} for validator ${selectedIndex}`)
          logAdded(selectedIndex, slot)
          attestation.slot = parseInt(slot)
          attestation.index = parseInt(index)
          attestation.position = position
          await db.put(attestationKey, attestation)
        }
      }
    }
  }

  if (logAddedList.length != logAddedSet.size)
    throw new Error(`Unexpected difference in logAdded: ${logAddedList.length} vs ${logAddedSet.size}`)
  if (logAddedSet.size)
    log(`Added ${logAddedSet.size} attestation duties in epoch ${epoch} between ${arrayMin(logAddedList.slice())} and ${arrayMax(logAddedList)}`)
}

async function processEpoch(epoch, validatorIds) {
  log(`Processing epoch ${epoch} with ${validatorIds.size} relevant validators`)

  if (!validatorIds.size) {
    console.warn(`${epoch} has no relevant active validators`)
    return
  }

  const rewardsOptions = rewardsOptionsForEpoch(validatorIds)

  const firstSlotInEpoch = epoch * slotsPerEpoch

  if (!running) return
  log(`Getting sync duties for ${epoch}`)

  const syncDutiesUrl = new URL(
    `/eth/v1/beacon/states/${firstSlotInEpoch}/sync_committees?epoch=${epoch}`,
    beaconRpcUrl
  )
  const syncValidators = await fetch(syncDutiesUrl).then(async res => {
    if (res.status === 400 && await res.json().then(j => j.message.endsWith("not activated for Altair")))
      return []
    if (res.status !== 200)
      throw new Error(`Got ${res.status} fetching sync duties for ${epoch}: ${await res.text()}`)
    const json = await res.json()
    return json.data.validators
  })
  for (const [position, validatorIndex] of syncValidators.entries()) {
    if (validatorIds.has(validatorIndex)) {
      const syncKey = `${chainId}/validator/${validatorIndex}/sync/${epoch}`
      const sync = db.get(syncKey) || {}
      if (!('position' in sync)) {
        log(`Adding sync duty for epoch ${epoch} for validator ${validatorIndex}`)
        sync.position = position
        sync.missed = []
        sync.rewards = []
        await db.put(syncKey, sync)
      }
    }
  }

  if (!running) return
  log(`Getting attestations and syncs for ${epoch}`)

  validatorIds.forEach(validatorIndex => {
    if (!(epoch <= db.get(`${chainId}/validator/${validatorIndex}/dutiesEpoch`)))
      throw new Error(`dutiesEpoch for ${validatorIndex} too low for ${epoch}`)
  })

  const validatorIdsArray = Array.from(validatorIds.keys())

  const logAdded = []
  let searchSlot = firstSlotInEpoch
  while (running && searchSlot < firstSlotInEpoch + slotsPerEpoch) {
    const blockUrl = new URL(
      `/eth/v1/beacon/blinded_blocks/${searchSlot}`,
      beaconRpcUrl
    )
    const blockData = await fetch(blockUrl).then(async res => {
      if (res.status === 404) {
        log(`Block for slot ${searchSlot} missing`)
        return { attestations: [] }
      }
      if (res.status !== 200)
        throw new Error(`Got ${res.status} fetching blinded block @ ${searchSlot}: ${await res.text()}`)
      const json = await res.json()
      return json.data.message.body
    })
    const attestations = blockData.attestations
    for (const {aggregation_bits, data: {slot, index, beacon_block_root, source, target}} of attestations) {
      const attestedBits = hexStringToBitlist(aggregation_bits)
      const attestationEpoch = epochOfSlot(parseInt(slot))
      await arrayPromises(
        validatorIdsArray.map(validatorIndex => async () => {
          const attestationKey = `${chainId}/validator/${validatorIndex}/attestation/${attestationEpoch}`
          const attestation = db.get(attestationKey)
          if (attestation?.slot == slot && attestation.index == index && !(attestation.attested?.slot <= searchSlot)) {
            if (attestedBits[attestation.position]) {
              attestation.attested = { slot: searchSlot, head: beacon_block_root, source, target }
              // log(`Adding attestation for ${slot} (${attestationEpoch}) for validator ${validatorIndex} @ ${searchSlot}`)
              logAdded.push({slot: searchSlot, validatorIndex, attestationEpoch})
              await db.put(attestationKey, attestation)
            }
          }
        }),
        NUM_INDEX_TASKS,
        // (numLeft) => log(`Getting attestations for ${slot} included in ${searchSlot}, ${numLeft} left`)
      )
    }
    if (blockData.sync_aggregate) {
      const syncBits = hexStringToBitvector(blockData.sync_aggregate.sync_committee_bits)
      await arrayPromises(
        validatorIdsArray.map(validatorIndex => async () => {
          const syncKey = `${chainId}/validator/${validatorIndex}/sync/${epoch}`
          const sync = db.get(syncKey)
          if (sync && !syncBits[sync.position] && !sync.missed.includes(searchSlot)) {
            sync.missed.push(searchSlot)
            await db.put(syncKey, sync)
          }
        }),
        NUM_INDEX_TASKS,
        // (numLeft) => log(`Getting sync duties for ${epoch}, ${numLeft} left`)
      )

      const syncRewardsUrl = new URL(
        `/eth/v1/beacon/rewards/sync_committee/${searchSlot}`,
        beaconRpcUrl
      )
      const syncRewards = await fetch(syncRewardsUrl, rewardsOptions).then(async res => {
        if (res.status !== 200)
          throw new Error(`Got ${res.status} fetching sync rewards @ ${searchSlot}: ${await res.text()}`)
        const json = await res.json()
        return json.data
      })
      for (const {validator_index, reward} of syncRewards) {
        const syncKey = `${chainId}/validator/${validator_index}/sync/${epoch}`
        const sync = db.get(syncKey)
        if (!sync) {
          if (reward !== '0')
            throw new Error(`Non-zero reward ${reward} but no sync object at ${syncKey}`)
          continue
        }
        if (sync.rewards.every(({slot}) => slot != searchSlot)) {
          // log(`Adding sync reward for ${searchSlot} for validator ${validator_index}: ${reward}`)
          logAdded.push({slot: searchSlot, validatorIndex: validator_index, reward})
          sync.rewards.push({slot: searchSlot, reward})
          await db.put(syncKey, sync)
        }
      }
    }

    searchSlot++
  }
  const addedAttestations = logAdded.filter(x => 'attestationEpoch' in x).length
  if (addedAttestations) {
    log(`Added ${addedAttestations} attestations for ${epoch}`)
    if (addedAttestations < logAdded.length)
      log(`Added ${logAdded.length - addedAttestations} sync rewards for ${epoch}`)
  }

  if (!running) return
  log(`Getting attestation rewards for ${epoch}`)

  logAdded.splice(0, Infinity)
  const attestationRewardsUrl = new URL(
    `/eth/v1/beacon/rewards/attestations/${epoch}`,
    beaconRpcUrl
  )
  const attestationRewards = await fetch(attestationRewardsUrl, rewardsOptions).then(async res => {
    if (res.status !== 200)
      throw new Error(`Got ${res.status} fetching attestation rewards in epoch ${epoch}: ${await res.text()}`)
    const json = await res.json()
    return json.data
  })
  for (const {validator_index, head, target, source, inactivity} of attestationRewards.total_rewards) {
    if (!running) break
    const attestationKey = `${chainId}/validator/${validator_index}/attestation/${epoch}`
    const attestation = db.get(attestationKey)
    if (attestation && !('reward' in attestation && 'ideal' in attestation)) {
      const effectiveBalanceUrl = new URL(
        `/eth/v1/beacon/states/${firstSlotInEpoch}/validators/${validator_index}`,
        beaconRpcUrl
      )
      const effectiveBalance = await fetch(effectiveBalanceUrl).then(async res => {
        if (res.status !== 200)
          throw new Error(`Got ${res.status} fetching effective balance in ${firstSlotInEpoch} for ${validator_index}: ${await res.text()}`)
        const json = await res.json()
        return json.data.validator.effective_balance
      })
      attestation.reward = {head, target, source, inactivity}
      const ideal = attestationRewards.ideal_rewards.find(x => x.effective_balance === effectiveBalance)
      if (!ideal) throw new Error(`Could not get ideal rewards for ${firstSlotInEpoch} ${validator_index} with ${effectiveBalance}`)
      attestation.ideal = {}
      for (const key of Object.keys(attestation.reward))
        attestation.ideal[key] = ideal[key]
      // log(`Adding attestation reward for epoch ${epoch} for validator ${validator_index}: ${Object.entries(attestation.reward)} / ${Object.entries(attestation.ideal)}`)
      logAdded.push(validator_index)
      await db.put(attestationKey, attestation)
    }
  }
  log(`Added ${logAdded.length} attestation rewards for epoch ${epoch}`)

  if (!running) return
  log(`Getting proposals for ${epoch}`)

  const proposalUrl = new URL(
    `/eth/v1/validator/duties/proposer/${epoch}`,
    beaconRpcUrl
  )
  const proposals = await fetch(proposalUrl).then(async res => {
    if (res.status !== 200)
      throw new Error(`Got ${res.status} fetching proposal duties for epoch ${epoch}: ${await res.text()}`)
    const json = await res.json()
    return json.data
  })
  for (const {validator_index, slot} of proposals) {
    if (!running) break
    if (validatorIds.has(validator_index)) {
      const proposalKey = `${chainId}/validator/${validator_index}/proposal/${slot}`
      const proposal = db.get(proposalKey) || {}
      if (!('reward' in proposal)) {
        const proposalRewardUrl = new URL(`/eth/v1/beacon/rewards/blocks/${slot}`, beaconRpcUrl)
        const response = await fetch(proposalRewardUrl)
        if (response.status === 404) {
          log(`Adding missed proposal for ${validator_index} @ ${slot}`)
          proposal.missed = true
          proposal.reward = '0'
        }
        else if (response.status === 200) {
          log(`Adding proposal reward for ${validator_index} @ ${slot}`)
          const reward = await response.json().then(j => j.data.total)
          proposal.reward = reward
        }
        else throw new Error(`Got ${response.status} fetching block rewards @ ${slot}`)
        await db.put(proposalKey, proposal)
      }
    }
  }
}

const tasks = []

async function processEpochsLoop(finalizedSlot, dutiesOnly) {
  const uptoKeyBase = dutiesOnly ? `dutiesEpoch` : `nextEpoch`

  const startKey = STANDARD_START_EPOCH ? '' : `/${OVERRIDE_START_EPOCH}`
  const uptoKey = `${uptoKeyBase}${startKey}`
  const startDefault = STANDARD_START_EPOCH ? 0 : OVERRIDE_START_EPOCH

  const alreadyOverridden = new Set()

  if (!STANDARD_START_EPOCH) {
    let changed
    for (const validatorIndex of validatorActivationEpochs.keys()) {
      const standardKey = `${chainId}/validator/${validatorIndex}/nextEpoch`
      const standardNextEpoch = db.get(standardKey)
      if (standardNextEpoch >= OVERRIDE_START_EPOCH) {
        const overrideKey = `${chainId}/validator/${validatorIndex}/nextEpoch${startKey}`
        const overrideNextEpoch = db.get(overrideKey)
        const actionMsg = overrideNextEpoch > standardNextEpoch ?
          `: merging up to ${overrideNextEpoch}` :
          (overrideNextEpoch ?
            `, but ${overrideNextEpoch} is also less so will delete it` :
            ' and has no overrideNextEpoch so will use standard')
        log(`Standard nextEpoch for ${validatorIndex}, ${standardNextEpoch}, has reached ${OVERRIDE_START_EPOCH}${actionMsg}`)
        if (overrideNextEpoch > standardNextEpoch) {
          await db.put(standardKey, overrideNextEpoch)
          await db.remove(overrideKey)
          changed = true
        }
        else if (overrideNextEpoch) {
          await db.remove(overrideKey)
          changed = true
        }
        else {
          alreadyOverridden.add(validatorIndex)
        }
      }
    }
    if (changed) {
      log(`Changed nextEpoch overrides, exiting...`)
      await cleanup()
      process.exit()
    }
  }

  const validatorNextEpochs = Array.from(validatorActivationEpochs.entries()).map(
    ([validatorIndex, activationEpoch]) => [
      validatorIndex,
      Math.max(
        db.get(
          `${chainId}/validator/${validatorIndex}/${alreadyOverridden.has(validatorIndex) ? uptoKeyBase : uptoKey}`
        ) ?? startDefault,
        activationEpoch
      )
    ]
  )

  const startEpoch = arrayMin(validatorNextEpochs.map(([id, nextEpoch]) => nextEpoch))
  log(`Calculated startEpoch for ${uptoKey} as ${startEpoch}`)

  const finalEpoch = epochOfSlot(finalizedSlot - 1)

  if (finalEpoch < startEpoch) {
    if (STANDARD_FINAL_SLOT || dutiesOnly) {
      log(`Start epoch ${startEpoch} greater than finalized ${finalEpoch}, continuing...`)
      return
    }
    else {
      log(`Reached final epoch ${finalEpoch}, exiting...`)
      await cleanup()
      process.exit()
    }
  }

  log(`Getting ${dutiesOnly ? 'attestation duties' : 'data'} for epochs ${startEpoch} through ${finalEpoch}`)

  const epochsToProcess = Array.from(Array(finalEpoch - startEpoch + 1).keys()).map(x => startEpoch + x)
  const pendingEpochs = epochsToProcess.slice()

  const processMsg = dutiesOnly ? 'attestation duties' : 'remaining data'

  const initialTotal = epochsToProcess.length
  const initialTime = new Date()

  while (running && epochsToProcess.length) {
    const epochsProcessed = initialTotal - epochsToProcess.length
    const millisecondsPassed = new Date() - initialTime
    const rate = millisecondsPassed/epochsProcessed
    const rateMinutes = (rate/60e3).toFixed(0)
    const rateMillisecondsLeft = rate - (rateMinutes * 60e3)
    const rateSecondsStr = 0 <= rateMillisecondsLeft ? ` ${(rateMillisecondsLeft/1000).toFixed(2)}s` : ''
    const rateStr = rate >= 60e3 ? `${rateMinutes}m${rateSecondsStr}` : `${(rate/1000).toFixed(2)}s`
    const timingMsg = epochsProcessed ? `, averaging ${rateStr} per epoch` : ''
    const plural = epochsToProcess.length == 1 ? '' : 's'
    log(`${epochsToProcess.length} epoch${plural} left to process ${processMsg}${timingMsg}`)
    const epoch = epochsToProcess.shift()
    const validatorIds = getValidatorsIdsForEpoch(validatorNextEpochs, epoch)
    const state = {}
    const onCompletion = async () => {
      const epochIndex = pendingEpochs.indexOf(epoch)
      pendingEpochs.splice(epochIndex, 1)
      if (epochIndex == 0) {
        const updated = []
        const nextEpoch = epoch + 1
        for (const validatorIndex of validatorIds.keys()) {
          if (!running) break
          const nextEpochKey = `${chainId}/validator/${validatorIndex}/${alreadyOverridden.has(validatorIndex) ? uptoKeyBase : uptoKey}`
          if ((db.get(nextEpochKey) || startDefault) < nextEpoch) {
            updated.push(validatorIndex)
            await db.put(nextEpochKey, nextEpoch)
          }
        }
        if (updated.length)
          log(`Updated ${uptoKey} to ${nextEpoch} for ${updated.length} validators from ${updated.at(0)} to ${updated.at(-1)}`)
      }
      state.resolved = true
      log(`Task for ${epoch} completed`)
    }
    const fn = dutiesOnly ? getAttestationDuties : processEpoch
    tasks.push({
      state, task: fn(epoch, validatorIds).then(onCompletion)
    })
    while (tasks.length >= NUM_EPOCH_TASKS) {
      await Promise.race(tasks.map(({task}) => task))
      filterResolved(tasks)
    }
  }
}

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

  if (!running) return

  await arrayPromises(
    validatorIdsToProcess.map(validatorIndex =>
      async () => {
        if (!(0 <= parseInt(validatorIndex))) return
        const epoch = epochFromActivationInfo(await getActivationInfo(validatorIndex))
        if (typeof epoch == 'number')
          validatorActivationEpochs.set(validatorIndex, epoch)
        else
          log(`Skipping ${validatorIndex} with activation epoch ${epoch}`)
      }
    ),
    MAX_BEACON_RANGE,
    (numLeft) => log(`Getting activationInfo, ${numLeft} validators left`)
  )

  const finalizedSlot = STANDARD_FINAL_SLOT ? await getFinalizedSlot() : OVERRIDE_FINAL_SLOT
  if (STANDARD_START_EPOCH) await processEpochsLoop(finalizedSlot, true)
  if (!DUTIES_ONLY) await processEpochsLoop(finalizedSlot, false)
}

async function cleanup() {
  running = false
  interruptArrayPromises()
  log(`Removing listeners...`)
  await provider.removeAllListeners('block')
  await blockLock
  log(`Awaiting tasks...`)
  await Promise.allSettled(tasks)
  await finishMoreTasks()
  log(`Closing db...`)
  await db.close()
}

process.on('SIGINT', async () => {
  log(`Received interrupt...`)
  if (!running) {
    log(`Alreading shutting down...`)
    return
  }
  await cleanup()
  process.exit()
})

while (true) {
  await processEpochs()
  await new Promise(resolve =>
    setTimeout(resolve, secondsPerSlot * slotsPerEpoch * 1000)
  )
}
