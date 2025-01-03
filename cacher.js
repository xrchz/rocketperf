import 'dotenv/config'
import { fetch, setGlobalDispatcher, Agent } from 'undici'
import { ethers } from 'ethers'
import { db, dbFor, closeAllDBs, provider, chainId, beaconRpcUrl, log, multicall, secondsPerSlot, rocketStorage,
         timeSlotConvs, slotsPerEpoch, epochOfSlot, minipoolAbi, arrayMin, arrayMax,
         finishMoreTasks, arrayPromises, interruptArrayPromises, filterResolved,
         getRocketAddressAt, minipoolsByPubkey, getIndexFromPubkey,
         getMinipoolByPubkey, getFinalizedSlot, getPubkeyFromIndex,
         rocketMinipoolManager, FAR_FUTURE_EPOCH, epochFromActivationInfo
       } from './lib.js'
const {timeToSlot, slotToTime} = timeSlotConvs(chainId)

setGlobalDispatcher(new Agent({ connect: { timeout: 60e3 } }) )

const MAX_QUERY_RANGE = parseInt(process.env.MAX_QUERY_RANGE) || 1000
const MAX_BEACON_RANGE = parseInt(process.env.MAX_BEACON_RANGE) || 100

const NUM_EPOCH_TASKS = parseInt(process.env.NUM_EPOCH_TASKS) || 16
const NUM_INDEX_TASKS = parseInt(process.env.NUM_INDEX_TASKS) || 2048

const OVERRIDE_START_EPOCH = parseInt(process.env.OVERRIDE_START_EPOCH)
const STANDARD_START_EPOCH = isNaN(OVERRIDE_START_EPOCH)
const DUTIES_ONLY = !!process.env.DUTIES_ONLY
if (DUTIES_ONLY && !STANDARD_START_EPOCH)
  throw new Error('DUTIES_ONLY cannot use OVERRIDE_START_EPOCH')

const OVERRIDE_FINAL_EPOCH = parseInt(process.env.OVERRIDE_FINAL_EPOCH)
const STANDARD_FINAL_SLOT = isNaN(OVERRIDE_FINAL_EPOCH)

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

const rocketStorageGenesisBlockByChain = {
  1: 13325233
}
const statusStaking = 2n
const statusDissolved = 4n

const rocketStorageGenesisBlock = rocketStorageGenesisBlockByChain[chainId]

let withdrawalAddressBlock = db.get([chainId,'withdrawalAddressBlock'])
if (!withdrawalAddressBlock) withdrawalAddressBlock = rocketStorageGenesisBlock

async function updateWithdrawalAddresses(finalizedBlockNumber) {
  while (withdrawalAddressBlock < finalizedBlockNumber) {
    if (!running) break
    const min = withdrawalAddressBlock
    const max = Math.min(withdrawalAddressBlock + MAX_QUERY_RANGE, finalizedBlockNumber)
    log(`Processing withdrawal addresses ${min}...${max}`)
    const logs = await rocketStorage.queryFilter('NodeWithdrawalAddressSet', min, max)
    for (const entry of logs) {
      if (!running) break
      const nodeAddress = entry.args[0]
      const withdrawalAddress = entry.args[1]
      const key = [chainId,'withdrawalAddress',withdrawalAddress]
      log(`Processing withdrawalAddress ${withdrawalAddress}`)
      await db.transaction(() => {
        const nodeAddresses = db.get(key) || []
        if (!nodeAddresses.includes(nodeAddress)) {
          nodeAddresses.push(nodeAddress)
          db.put(key, nodeAddresses)
        }
      })
    }
    withdrawalAddressBlock = max
    await db.put([chainId,'withdrawalAddressBlock'], withdrawalAddressBlock)
  }
}

// TODO: delete minipoolsByPubkeyCount from db

let minipoolsBlock = db.get([chainId,'minipoolsBlock'])
if (!minipoolsBlock) minipoolsBlock = rocketStorageGenesisBlock

async function updateMinipoolPubkeys(finalizedBlockNumber) {
  while (minipoolsBlock < finalizedBlockNumber) {
    if (!running) break
    const min = minipoolsBlock
    const max = Math.min(minipoolsBlock + MAX_QUERY_RANGE, finalizedBlockNumber)
    log(`Processing minipool creations ${min}...${max}`)
    const addresses = await Promise.all([min, max].map(blockTag => getRocketAddressAt('rocketMinipoolManager', blockTag)))
    if (addresses[0] == addresses[1]) addresses.pop()
    const contracts = addresses.map(addr => new ethers.Contract(addr, ['event MinipoolCreated(address indexed minipool, address indexed node, uint256 time)'], provider))
    const logs = await Promise.all(contracts.map(contract => contract.queryFilter('MinipoolCreated', min, max))).then(a => a.flat())
    const minipoolAddresses = logs.map(entry => entry.args[0])
    const pubkeys = await multicall(
      minipoolAddresses.map(minipoolAddress => ({
        contract: rocketMinipoolManager,
        fn: 'getMinipoolPubkey',
        args: [minipoolAddress]
      }))
    )
    for (const [i, minipoolAddress] of minipoolAddresses.entries()) {
      if (!running) break
      const pubkey = pubkeys[i]
      const currentEntry = minipoolsByPubkey[pubkey]
      if (currentEntry && currentEntry != minipoolAddress) {
        const currentMinipool = new ethers.Contract(currentEntry, minipoolAbi, provider)
        const pendingMinipool = new ethers.Contract(minipoolAddress, minipoolAbi, provider)
        const currentStatus = await currentMinipool.getStatus()
        const pendingStatus = await pendingMinipool.getStatus()
        if (currentStatus == pendingStatus)
          await cleanupThenError(`Duplicate minipools with status ${currentStatus} for ${pubkey}: ${minipoolAddress} vs ${currentEntry}`)
        const activeAddress = currentStatus == statusDissolved ? minipoolAddress :
                              pendingStatus == statusDissolved ? currentEntry : null
        if (!activeAddress)
          await cleanupThenError(`Duplicate minipools for ${pubkey} with neither dissolved: ${minipoolAddress} vs ${currentEntry}`)
        log(`Found duplicate minipools for ${pubkey}: ${minipoolAddress} vs ${currentEntry}. Keeping ${activeAddress}.`)
        minipoolsByPubkey[pubkey] = activeAddress
      }
      else {
        const currentMinipool = new ethers.Contract(minipoolAddress, minipoolAbi, provider)
        const currentStatus = await currentMinipool.getStatus()
        if (currentStatus == statusDissolved)
          log(`Ignoring minipool ${minipoolAddress} for ${pubkey} because it is dissolved`)
        else
          minipoolsByPubkey[pubkey] = minipoolAddress
      }
    }
    minipoolsBlock = max
    await db.transaction(() => {
      db.put([chainId,'minipoolsBlock'], minipoolsBlock)
      db.put([chainId,'minipoolsByPubkey'], minipoolsByPubkey)
    })
  }
}

const LISTEN_BLOCKS = (
  process.env.ONLY_BLOCK_LISTENER ||
  (STANDARD_START_EPOCH && !DUTIES_ONLY &&
   !process.env.FIXUP_EPOCHS && !process.env.NO_BLOCK_LISTENER)
)

let blockLock
if (LISTEN_BLOCKS) {
  provider.addListener('block', async () => {
    if (!blockLock) {
      const finalizedBlockNumber = await provider.getBlock('finalized').then(b => b.number)
      blockLock = Promise.all([
        updateWithdrawalAddresses(finalizedBlockNumber),
        updateMinipoolPubkeys(finalizedBlockNumber)
      ])
      await blockLock
      blockLock = false
    }
  })
}

const fixAnkrPubkeyFromIndex = new Map([
  [1110070, '0xa79f1abda7e2c1fa37736a00e3da97d7d4c988a3293abff97bf18a946778a22c6de1778d67490a0ff2a01b9842923ec8'],
  [1117590, '0xb26b3440bfac9b7adbeb8ffa2d3351d6bf8b3c0cea8cd7e6d02da46c51f7f30b6cddbab7c7560b9810b990d044404758'],
  [1117589, '0xab7f239b5d85921249e0d587a0fea19e62b609e00b366403f32d8be179e1f9d0120c37ee34e1af2523d86cfde0e2ec1c'],
  [1129625, '0xaae335e3f300e81b3e67fc2695c94d894cc8ab51ed930e7f4c408d0f6ca125f513365b13026c94661533287409ff6871'],
  [1139387, '0x979b5256b865366b40f25ca537cd3caac31e5fb2a386a79d94a046f5f7aba507a19290b0cfb400f8824e038daa1c692b'],
  [1139388, '0xadf733840e05ffbb23dd354a0f090eba1c8569db2c1824bbeb9b2703aae6ea12c4810bb1d20b42edcba7494234370d1a']
])

async function getActivationInfo(validatorIndex) {
  const fullKey = [chainId,'validator',validatorIndex,'activationInfo']
  const dba = dbFor(fullKey)
  const key = fullKey.slice(2)
  const activationInfo = dba.get(key) ?? {}
  let changed = false
  if (!('beacon' in activationInfo)) {
    const path = `/eth/v1/beacon/states/finalized/validators/${validatorIndex}`
    const url = new URL(`${beaconRpcUrl}${path}`)
    const res = await tryfetch(url)
    const text = await res.clone().text()
    const json = text == 'not found' ? {data: {validator: {activation_epoch: FAR_FUTURE_EPOCH}}} :
      await res.json().catch(e => {
        console.error(`Error getting JSON from ${url}`)
        return cleanup().then(() => { throw e })
      })
    const epoch = parseInt(json?.data?.validator?.activation_epoch)
    if (!(0 <= epoch))
      await cleanupThenError(`Failed to get activation_epoch for ${validatorIndex}`)
    if (epoch == FAR_FUTURE_EPOCH)
      log(`Skipping setting unknown activation epoch for ${validatorIndex}`)
    else {
      activationInfo.beacon = epoch
      changed = true
    }
  }
  if (!('promoted' in activationInfo)) {
    const pubkey = await getPubkeyFromIndex(validatorIndex) // || fixAnkrPubkeyFromIndex.get(validatorIndex)
    const minipoolAddress = getMinipoolByPubkey(pubkey)
    const minipoolExists = await rocketMinipoolManager.getMinipoolExists(minipoolAddress)
    if (!minipoolExists)
      await cleanupThenError(`Validator ${validatorIndex} (${pubkey}) has no corresponding minipool (${minipoolAddress})`)
    const minipool = new ethers.Contract(minipoolAddress, minipoolAbi, provider)
    if (await minipool.getStatus().then(s => s == statusStaking)) {
      if (await minipool.getVacant().catch(revert => false))
        await cleanupThenError(`Minipool ${minipoolAddress} (validator ${validatorIndex}) is vacant`)
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
  if (changed) await dba.put(key, activationInfo)
  return activationInfo
}

const validatorActivationEpochs = new Map()

// TODO: ignore exited validators?
const getValidatorsIdsForEpoch = (validatorNextEpochs, epoch) => new Set(
  validatorNextEpochs.flatMap(
    ([id, nextEpoch]) => nextEpoch <= epoch ? [id] : []
  )
)

const postOptionsForEpoch = (validatorIds) => {
  const ids = Array.from(validatorIds.keys()).map(i => i.toString())
  const options = {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(ids)
  }
  const wrapped = {...options, body: JSON.stringify({ids})}
  return {postOptions: options, wrappedPostOptions: wrapped}
}

let processedMinipoolCount = 0

async function getAttestationDuties(epoch, validatorIds) {
  log(`Getting attestation duties for ${epoch}`)

  if (!validatorIds.size) {
    console.warn(`${epoch} has no relevant active validators`)
    return
  }

  const firstSlotInEpoch = epoch * slotsPerEpoch

  const attestationDutiesUrl = new URL(
    `${beaconRpcUrl}/eth/v1/beacon/states/${firstSlotInEpoch}/committees?epoch=${epoch}`
  )

  if (!running) return

  const logAddedSet = new Set()
  const logAddedList = []
  const logAdded = (index, slot) => {
    logAddedSet.add(index)
    logAddedList.push(slot)
  }

  const promises = []

  const committees = await tryfetch(attestationDutiesUrl).then(async res => {
    if (res.status !== 200)
      await cleanupThenError(`Got ${res.status} fetching attestation duties for ${epoch}: ${await res.text()}`)
    const json = await res.json()
    return json.data
  })
  for (const {index, slot, validators} of committees) {
    if (!running) break
    for (const [position, validator_index] of validators.entries()) {
      if (!running) break
      const selectedIndex = parseInt(validator_index)
      if (validatorIds.has(selectedIndex)) {
        const fullAttestationKey = [chainId,'validator',selectedIndex,'attestation',epoch]
        const dba = dbFor(fullAttestationKey)
        const attestationKey = fullAttestationKey.slice(2)
        const attestation = dba.get(attestationKey) || {}
        if (!('position' in attestation)) {
          // log(`Adding attestation duty @ ${slot} for validator ${selectedIndex}`)
          logAdded(selectedIndex, slot)
          attestation.slot = parseInt(slot)
          attestation.index = parseInt(index)
          attestation.position = position
          promises.push(dba.put(attestationKey, attestation))
        }
      }
    }
  }

  const timeKey = `putting attestation duties data for ${epoch}`
  console.time(timeKey)
  await Promise.all(promises)
  console.timeEnd(timeKey)

  if (logAddedList.length != logAddedSet.size)
    await cleanupThenError(`Unexpected difference in logAdded: ${logAddedList.length} vs ${logAddedSet.size}`)
  if (logAddedSet.size)
    log(`Added ${logAddedSet.size} attestation duties in epoch ${epoch} between ${arrayMin(logAddedList.slice())} and ${arrayMax(logAddedList)}`)
}

async function processEpoch(epoch, validatorIds) {
  log(`Processing epoch ${epoch} with ${validatorIds.size} relevant validators`)

  if (!validatorIds.size) {
    console.warn(`${epoch} has no relevant active validators`)
    return
  }

  const {postOptions, wrappedPostOptions} = postOptionsForEpoch(validatorIds)

  const firstSlotInEpoch = epoch * slotsPerEpoch

  if (!running) return
  log(`Getting sync duties for ${epoch}`)

  const promises = []
  const syncUpdates = new Map()

  const syncDutiesUrl = new URL(
    `${beaconRpcUrl}/eth/v1/beacon/states/${firstSlotInEpoch}/sync_committees?epoch=${epoch}`
  )
  const syncValidators = await tryfetch(syncDutiesUrl).then(async res => {
    if (res.status === 400 && await res.json().then(j => j.message.endsWith("not activated for Altair")))
      return []
    if (res.status !== 200)
      await cleanupThenError(`Got ${res.status} fetching ${syncDutiesUrl}: ${await res.text()}`)
    const json = await res.json()
    return json.data.validators
  })
  for (const [position, validator_index] of syncValidators.entries()) {
    const validatorIndex = parseInt(validator_index)
    if (validatorIds.has(validatorIndex)) {
      const fullSyncKey = [chainId,'validator',validatorIndex,'sync',epoch]
      const dbs = dbFor(fullSyncKey)
      const syncKey = fullSyncKey.slice(2)
      const sync = dbs.get(syncKey) || {}
      if (!('position' in sync)) {
        log(`Adding sync duty for epoch ${epoch} for validator ${validatorIndex}`)
        sync.position = position
        sync.missed = []
        sync.rewards = []
        syncUpdates.set(fullSyncKey.join('/'), {dbs, syncKey, sync})
      }
    }
  }

  if (!running) return
  log(`Getting attestations and syncs for ${epoch}`)

  for (const validatorIndex of validatorIds) {
    const key = [chainId,'validator',validatorIndex,'dutiesEpoch']
    if (!(epoch <= dbFor(key).get(key.slice(2))))
      await cleanupThenError(`dutiesEpoch for ${validatorIndex} too low for ${epoch}`)
  }

  const validatorIdsArray = Array.from(validatorIds.keys())

  const attestationUpdates = new Map()

  const logAdded = []
  let searchSlot = firstSlotInEpoch
  while (running && searchSlot < firstSlotInEpoch + slotsPerEpoch) {
    const blockUrl = new URL(
      `${beaconRpcUrl}/eth/v1/beacon/blinded_blocks/${searchSlot}`
    )
    const blockData = await tryfetch(blockUrl).then(async res => {
      if (res.status === 404) {
        log(`Block for slot ${searchSlot} missing`)
        return { attestations: [] }
      }
      if (res.status !== 200)
        await cleanupThenError(`Got ${res.status} fetching blinded block @ ${searchSlot}: ${await res.text()}`)
      const json = await res.json()
      return json.data.message.body
    })
    const attestations = blockData.attestations
    for (const {aggregation_bits, data: {slot, index, beacon_block_root, source, target}} of attestations) {
      const attestedBits = hexStringToBitlist(aggregation_bits)
      const attestationEpoch = epochOfSlot(parseInt(slot))
      validatorIdsArray.forEach(validatorIndex => {
        const fullAttestationKey = [chainId,'validator',validatorIndex,'attestation',attestationEpoch]
        const fullAttestationKeyStr = fullAttestationKey.join('/')
        const attestationKey = fullAttestationKey.slice(2)
        const dba = dbFor(fullAttestationKey)
        const attestation = attestationUpdates.get(fullAttestationKeyStr)?.attestation || dba.get(attestationKey)
        if (attestation?.slot == slot && attestation.index == index && !(attestation.attested?.slot <= searchSlot)) {
          if (attestedBits[attestation.position]) {
            attestation.attested = { slot: searchSlot, head: beacon_block_root, source, target }
            // log(`Adding attestation for ${slot} (${attestationEpoch}) for validator ${validatorIndex} @ ${searchSlot}`)
            logAdded.push({slot: searchSlot, validatorIndex, attestationEpoch})
            attestationUpdates.set(fullAttestationKeyStr, {dba, attestationKey, attestation})
          }
        }
      })
    }
    if (blockData.sync_aggregate) {
      const syncBits = hexStringToBitvector(blockData.sync_aggregate.sync_committee_bits)
      validatorIdsArray.forEach(validatorIndex => {
        const fullSyncKey = [chainId,'validator',validatorIndex,'sync',epoch]
        const syncKey = fullSyncKey.slice(2)
        const fullSyncKeyStr = fullSyncKey.join('/')
        const dbs = dbFor(fullSyncKey)
        const sync = syncUpdates.get(fullSyncKeyStr)?.sync || dbs.get(syncKey)
        if (sync && !syncBits[sync.position] && !sync.missed.includes(searchSlot)) {
          sync.missed.push(searchSlot)
          syncUpdates.set(fullSyncKeyStr, {dbs, syncKey, sync})
        }
      })

      const syncRewardsUrl = new URL(
        `${beaconRpcUrl}/eth/v1/beacon/rewards/sync_committee/${searchSlot}`
      )
      const syncRewards = await tryfetch(syncRewardsUrl, postOptions).then(async res => {
        if (res.status !== 200)
          await cleanupThenError(`Got ${res.status} fetching ${syncRewardsUrl}: ${await res.text()}`)
        const json = await res.json()
        return json.data
      })
      for (const {validator_index, reward} of syncRewards) {
        const validatorIndex = parseInt(validator_index)
        const fullSyncKey = [chainId,'validator',validatorIndex,'sync',epoch]
        const syncKey = fullSyncKey.slice(2)
        const fullSyncKeyStr = fullSyncKey.join('/')
        const dbs = dbFor(fullSyncKey)
        const sync = syncUpdates.get(fullSyncKeyStr)?.sync || dbs.get(syncKey)
        if (!sync) {
          if (reward !== '0')
            await cleanupThenError(`Non-zero reward ${reward} but no sync object at ${syncKey}`)
          continue
        }
        if (sync.rewards.every(({slot}) => slot != searchSlot)) {
          // log(`Adding sync reward for ${searchSlot} for validator ${validator_index}: ${reward}`)
          logAdded.push({slot: searchSlot, validatorIndex: validator_index, reward})
          sync.rewards.push({slot: searchSlot, reward})
          syncUpdates.set(fullSyncKeyStr, {dbs, syncKey, sync})
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

  syncUpdates.forEach(({dbs, syncKey, sync}) => promises.push(dbs.put(syncKey, sync)))

  if (!running) return
  log(`Getting attestation rewards for ${epoch}`)

  logAdded.splice(0, Infinity)
  const attestationRewardsUrl = new URL(
    `${beaconRpcUrl}/eth/v1/beacon/rewards/attestations/${epoch}`
  )
  const attestationRewards = await tryfetch(attestationRewardsUrl, postOptions).then(async res => {
    if (res.status !== 200)
      await cleanupThenError(`Got ${res.status} fetching ${attestationRewardsUrl}: ${await res.text()}`)
    const json = await res.json()
    return json.data
  })
  const effectiveBalancesUrl = new URL(
    `${beaconRpcUrl}/eth/v1/beacon/states/${firstSlotInEpoch}/validators`
  )
  const effectiveBalances = await tryfetch(effectiveBalancesUrl, wrappedPostOptions).then(async res => {
    if (res.status !== 200)
      await cleanupThenError(`Got ${res.status} fetching ${effectiveBalancesUrl}: ${await res.text()}`)
    const json = await res.json()
    return new Map(json.data.map(({index, validator: {effective_balance}}) => [index, effective_balance]))
  })
  for (const {validator_index, head, target, source, inactivity} of attestationRewards.total_rewards) {
    if (!running) break
    const fullAttestationKey = [chainId,'validator',parseInt(validator_index),'attestation',epoch]
    const attestationKey = fullAttestationKey.slice(2)
    const fullAttestationKeyStr = fullAttestationKey.join('/')
    const dba = dbFor(fullAttestationKey)
    const attestation = attestationUpdates.get(fullAttestationKeyStr)?.attestation || dba.get(attestationKey)
    if (attestation && !('reward' in attestation && 'ideal' in attestation)) {
      const effectiveBalance = effectiveBalances.get(validator_index)
      attestation.reward = {head, target, source, inactivity}
      const ideal = attestationRewards.ideal_rewards.find(x => x.effective_balance === effectiveBalance)
      if (!ideal) await cleanupThenError(`Could not get ideal rewards for ${firstSlotInEpoch} ${validator_index} with ${effectiveBalance}`)
      attestation.ideal = {}
      for (const key of Object.keys(attestation.reward))
        attestation.ideal[key] = ideal[key]
      // log(`Adding attestation reward for epoch ${epoch} for validator ${validator_index}: ${Object.entries(attestation.reward)} / ${Object.entries(attestation.ideal)}`)
      logAdded.push(validator_index)
      attestationUpdates.set(fullAttestationKeyStr, {dba, attestationKey, attestation})
    }
  }
  log(`Added ${logAdded.length} attestation rewards for epoch ${epoch}`)

  attestationUpdates.forEach(({dba, attestationKey, attestation}) =>
    promises.push(dba.put(attestationKey, attestation))
  )

  if (!running) return
  log(`Getting proposals for ${epoch}`)

  const proposalUrl = new URL(
    `${beaconRpcUrl}/eth/v1/validator/duties/proposer/${epoch}`
  )
  const proposals = await tryfetch(proposalUrl).then(async res => {
    if (res.status !== 200)
      await cleanupThenError(`Got ${res.status} fetching proposal duties for epoch ${epoch}: ${await res.text()}`)
    const json = await res.json()
    return json.data
  })
  for (const {validator_index, slot} of proposals) {
    if (!running) break
    const validatorIndex = parseInt(validator_index)
    if (validatorIds.has(validatorIndex)) {
      const fullProposalKey = [chainId,'validator',validatorIndex,'proposal',parseInt(slot)]
      const dbp = dbFor(fullProposalKey)
      const proposalKey = fullProposalKey.slice(2)
      const proposal = dbp.get(proposalKey) || {}
      if (!('reward' in proposal)) {
        const proposalRewardUrl = new URL(`${beaconRpcUrl}/eth/v1/beacon/rewards/blocks/${slot}`)
        const response = await tryfetch(proposalRewardUrl)
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
        else await cleanupThenError(`Got ${response.status} fetching block rewards @ ${slot}`)
        promises.push(dbp.put(proposalKey, proposal))
      }
    }
  }

  const timeKey = `putting data for ${epoch}`
  console.time(timeKey)
  await Promise.all(promises)
  console.timeEnd(timeKey)
}

const tasks = []

async function processEpochsLoop(finalizedSlot, dutiesOnly) {
  const uptoKeyBase = dutiesOnly ? `dutiesEpoch` : `nextEpoch`

  const startKey = STANDARD_START_EPOCH ? [] : [OVERRIDE_START_EPOCH]
  const uptoKey = [uptoKeyBase, ...startKey]
  const startDefault = STANDARD_START_EPOCH ? 0 : OVERRIDE_START_EPOCH

  const alreadyOverridden = new Set()

  if (!STANDARD_START_EPOCH) {
    const changes = []
    for (const [validatorIndex, activationEpoch] of validatorActivationEpochs.entries()) {
      const fullStandardKey = [chainId,'validator',validatorIndex,'nextEpoch']
      const standardKey = fullStandardKey.slice(2)
      const dbv = dbFor(fullStandardKey)
      const standardExists = dbv.get(standardKey)
      const standardNextEpoch = standardExists ?? activationEpoch
      if (!standardExists) await dbv.put(standardKey, standardNextEpoch)
      if (standardNextEpoch >= OVERRIDE_START_EPOCH) {
        const overrideKey = [validatorIndex,'nextEpoch',...startKey]
        const overrideNextEpoch = dbv.get(overrideKey)
        const actionMsg = overrideNextEpoch >= standardNextEpoch ?
          `: merging up to ${overrideNextEpoch}` :
          (overrideNextEpoch ?
            `, but override, ${overrideNextEpoch}, is less so will delete it` :
            ' and has no overrideNextEpoch so will use standard')
        log(`Standard nextEpoch for ${validatorIndex}, ${standardNextEpoch}, has reached ${OVERRIDE_START_EPOCH}${actionMsg}`)
        if (overrideNextEpoch >= standardNextEpoch) {
          changes.push(dbv.transaction(
            () => {
              dbv.put(standardKey, overrideNextEpoch)
              dbv.remove(overrideKey)
            })
          )
        }
        else if (overrideNextEpoch) {
          changes.push(dbv.remove(overrideKey))
        }
        else {
          alreadyOverridden.add(validatorIndex)
        }
      }
    }
    if (changes.length) {
      log(`Changed nextEpoch overrides...`)
      await Promise.all(changes)
      await cleanup()
      process.exit()
    }
  }

  // TODO: ignore exited validators?
  // TODO: sort by index?
  const validatorNextEpochs = Array.from(validatorActivationEpochs.entries()).map(
    ([validatorIndex, activationEpoch]) => [
      validatorIndex,
      Math.max(
        dbFor([chainId,'validator',validatorIndex]).get(
          [validatorIndex,...(alreadyOverridden.has(validatorIndex) ? [uptoKeyBase] : uptoKey)]
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
  let pendingEpochsLock = Promise.resolve()

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
      if (!running) {
        state.resolved = true
        return
      }
      const epochIndex = await pendingEpochsLock.then(() => {
        pendingEpochsLock = new Promise(resolve => {
          const epochIndex = pendingEpochs.indexOf(epoch)
          pendingEpochs.splice(epochIndex, 1)
          return resolve(epochIndex)
        })
        return pendingEpochsLock
      })
      if (epochIndex == 0) {
        const promises = []
        const updated = []
        const nextEpoch = epoch + 1
        for (const validatorIndex of validatorIds.keys()) {
          if (!running) break
          const fullNextEpochKey = [chainId,'validator',validatorIndex,...(alreadyOverridden.has(validatorIndex) ? [uptoKeyBase] : uptoKey)]
          const dbv = dbFor(fullNextEpochKey)
          const nextEpochKey = fullNextEpochKey.slice(2)
          const currentValue = dbv.get(nextEpochKey)
          if (!currentValue || currentValue < nextEpoch) {
            updated.push(validatorIndex)
            promises.push(dbv.put(nextEpochKey, nextEpoch))
          }
        }
        if (!running) {
          state.resolved = true
          return
        }
        if (updated.length)
          log(`Updated ${uptoKey} to ${nextEpoch} for ${updated.length} validators from ${updated.at(0)} to ${updated.at(-1)}`)
        const timeKey = `putting nextEpoch for ${epoch}`
        console.time(timeKey)
        await Promise.all(promises)
        console.timeEnd(timeKey)
      }
      state.resolved = true
      log(`Task for ${epoch} completed (was pendingEpochs[${epochIndex}])`)
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
  await Promise.allSettled(tasks.map(({task}) => task))
}

async function processEpochs() {
  const pubkeys = Object.keys(minipoolsByPubkey)
  const targetMinipoolCount = pubkeys.length

  if (!running) return

  log(`targetMinipoolCount: ${targetMinipoolCount}, processedMinipoolCount: ${processedMinipoolCount}`)

  const validatorIdsToProcess = await arrayPromises(
    pubkeys.slice(processedMinipoolCount, targetMinipoolCount).map(
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
        if (!(0 <= validatorIndex)) return
        // if ([if you need to ignore validators].includes(validatorIndex)) return
        // if ([19026].includes(validatorIndex)) return
        const epoch = await getActivationInfo(validatorIndex).then(x => epochFromActivationInfo(x)).catch(y => `ERROR ${y}`)
        if (typeof epoch == 'number')
          validatorActivationEpochs.set(validatorIndex, epoch)
        else
          log(`Skipping ${validatorIndex} with activation epoch ${epoch}`)
      }
    ),
    MAX_BEACON_RANGE,
    (numLeft) => log(`Getting activationInfo, ${numLeft} validators left`)
  )

  if (process.env.FIXUP_EPOCHS) return

  const finalizedSlot = STANDARD_FINAL_SLOT ? await getFinalizedSlot() : OVERRIDE_FINAL_EPOCH * slotsPerEpoch
  log(`Using finalizedSlot ${finalizedSlot}`)
  if (STANDARD_START_EPOCH) await processEpochsLoop(finalizedSlot, true)
  if (!DUTIES_ONLY) await processEpochsLoop(finalizedSlot, false)
}

const tryfetch = parseInt(process.env.LOG_FETCH) ?
  (...args) => {
    console.time(args[0])
    const r = fetch(...args).catch((e) => cleanup().then(() => { throw e }))
    console.timeEnd(args[0])
    return r
  } :
  (...args) => fetch(...args).catch((e) => cleanup().then(() => { throw e }))

const cleanupThenError = (s) => {
  console.log(`Got to cleanupThenError with ${s}`)
  console.error(s)
  return cleanup().then(() => { throw new Error(s) })
}

async function cleanup() {
  if (!running) return
  running = false
  interruptArrayPromises()
  log(`Removing listeners...`)
  await provider.removeAllListeners('block')
  await blockLock
  log(`Awaiting tasks...`)
  await Promise.allSettled(tasks.map(({task}) => task))
  log(`Awaiting more tasks...`)
  await finishMoreTasks()
  log(`Closing dbs...`)
  await closeAllDBs(chainId)
}

process.on('SIGINT', async () => {
  log(`Received interrupt...`)
  if (!running) {
    log(`Already shutting down...`)
    return
  }
  await cleanup()
  process.exit()
})

import { createInterface } from 'node:readline'
import { createReadStream } from 'node:fs'

if (process.env.FIXUP_EPOCHS) {
  const getInts = async (s) => {
    if (s.endsWith('.txt')) {
      const vs = new Set()
      for await (const line of createInterface({input: createReadStream(s)}))
        vs.add(parseInt(line))
      return vs
    }
    else if (s)
      return new Set(s.split(',').map(i => parseInt(i)))
    else
      return new Set()
  }
  const epochs = await getInts(process.env.FIXUP_EPOCHS)
  const validatorIds = await getInts(process.env.FIXUP_VALIDATORS || '')
  const fn = DUTIES_ONLY ? getAttestationDuties : processEpoch
  if (!validatorIds.size) {
    await processEpochs() // to fill validatorActivationEpochs
  }
  for (const epoch of epochs.values()) {
    if (!running) break
    const state = {}
    const onCompletion = () => { state.resolved = true }
    const vids = validatorIds.size ? validatorIds : new Set(
      Array.from(validatorActivationEpochs.entries()).flatMap(([id, activationEpoch]) =>
        activationEpoch <= epoch ? [id] : [])
    )
    tasks.push({
      state, task: fn(epoch, vids).then(onCompletion)
    })
    while (tasks.length >= NUM_EPOCH_TASKS) {
      await Promise.race(tasks.map(({task}) => task))
      filterResolved(tasks)
    }
  }
  await Promise.allSettled(tasks.map(({task}) => task))
  await cleanup()
}
else if (!process.env.ONLY_BLOCK_LISTENER) {
  while (running) {
    await processEpochs()
    if (STANDARD_FINAL_SLOT)
      await new Promise(resolve =>
        setTimeout(resolve, secondsPerSlot * slotsPerEpoch * 1000)
      )
  }
}
