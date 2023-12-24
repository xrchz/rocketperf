import 'dotenv/config'
import { ethers } from 'ethers'
import { db, provider, chainId, beaconRpcUrl, log, multicall,
         timeSlotConvs, slotsPerEpoch, epochOfSlot, minipoolAbi,
         minipoolsByPubkeyCount, minipoolsByPubkey, minipoolCount,
         incrementMinipoolsByPubkeyCount, getMinipoolByPubkey,
         getFinalizedSlot, getPubkeyFromIndex, rocketMinipoolManager
       } from './lib.js'

const {timeToSlot, slotToTime} = timeSlotConvs(chainId)

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

const MAX_QUERY_RANGE = 1000

const rocketStorage = new ethers.Contract(
  await provider.resolveName('rocketstorage.eth'),
  ['event NodeWithdrawalAddressSet (address indexed node, address indexed withdrawalAddress, uint256 time)'],
  provider
)

const rocketStorageGenesisBlockByChain = {
  1: 13325233
}

const rocketStorageGenesisBlock = rocketStorageGenesisBlockByChain[chainId]

const finalizedBlockNumber = await provider.getBlock('finalized').then(b => b.number)
const finalizedSlot = await getFinalizedSlot()

let withdrawalAddressBlock = db.get(`${chainId}/withdrawalAddressBlock`)
if (!withdrawalAddressBlock) withdrawalAddressBlock = rocketStorageGenesisBlock

while (withdrawalAddressBlock < finalizedBlockNumber) {
  const min = withdrawalAddressBlock
  const max = Math.min(withdrawalAddressBlock + MAX_QUERY_RANGE, finalizedBlockNumber)
  console.log(`Processing withdrawal addresses ${min}...${max}`)
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

// TODO: addListener for NodeWithdrawalAddressSet

const statusDissolved = 4n

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
    else
      minipoolsByPubkey.set(pubkey, minipoolAddress)
  }
  incrementMinipoolsByPubkeyCount(n)
  log(`Got pubkeys for ${minipoolsByPubkeyCount} minipools`)
}
await db.put(`${chainId}/minipoolsByPubkeyCount`, minipoolsByPubkeyCount)
await db.put(`${chainId}/minipoolsByPubkey`, minipoolsByPubkey)

// TODO: add listener for minipool added

async function getActivationInfo(validatorIndex) {
  const key = `${chainId}/validator/${validatorIndex}/activationInfo`
  const activationInfo = db.get(key) ?? {}
  let changed = false
  if (!('beacon' in activationInfo)) {
    const path = `/eth/v1/beacon/states/finalized/validators/${validatorIndex}`
    const url = new URL(path, beaconRpcUrl)
    const res = await fetch(url)
    const json = await res.json()
    if (!(0 <= parseInt(json?.data?.validator?.activation_epoch)))
      throw new Error(`Failed to get activation_epoch for ${validatorIndex}`)
    activationInfo.beacon = parseInt(json.data.validator.activation_epoch)
    changed = true
  }
  if (!('promoted' in activationInfo)) {
    const pubkey = await getPubkeyFromIndex(validatorIndex)
    const minipoolAddress = getMinipoolByPubkey(pubkey)
    const minipoolExists = await rocketMinipoolManager.getMinipoolExists(minipoolAddress)
    if (!minipoolExists)
      throw new Error(`Validator ${validatorIndex} (${pubkey}) has no corresponding minipool (${minipoolAddress})`)
    const minipool = new ethers.Contract(minipoolAddress, minipoolAbi, provider)
    if (await minipool.getVacant())
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

// TODO: get these from all rocketpool minipools
const validatorIdsToProcess = [
  '509045',
  '509048',
  '509049',
  '509076',
  '509077',
  '509644',
  '517312',
  '581781',
  '915834',
  '915835',
  '915836',
  '915837',
  '915838',
  '918039',
  '918040',
  '263489',
  '263491',
  '410932',
  '583656',
  '583658',
  '583659',

  '178607',
  '299145',
  '299563',
  '315096',
  '315416',
  '331665',
  '395350',
  '396666',
  '396667',
  '405639',
  '405712',
  '409340',
  '410810',
  '410851',
  '410896',
  '411937',
  '415621',
  '466392',
  '485042',
  '485428',
  '518517',
  '879631',
  '883986',
  '890986',
  '891017',
  '893010',
  '893011',
  '893047',
  '895461',
  '895462',
  '898190',
  '898195',
  '898198',
  '902988',
  '915547',
  '932768',
  '940859',
  '940860',
  '943025',
  '943026',
  '954862',
  '968183',
  '975220',
  '1017625',
  '1017762',
  '1017767',
  '1034429',
  '1034438',
  '1043986',
  '1044502',
  '1089033',
  '1089163',
  '1089178',
  '1089365',
  '1089776'
]

const epochFromActivationInfo = activationInfo =>
  activationInfo.promoted ?
    epochOfSlot(activationInfo.promoted) :
    activationInfo.beacon

// TODO: get everything already in the db + validatorIdsToProcess
const validatorIdsToConsider = new Map()
for (const validatorIndex of validatorIdsToProcess) {
  log(`Getting activation epoch for ${validatorIndex}`)
  validatorIdsToConsider.set(
    validatorIndex,
    epochFromActivationInfo(await getActivationInfo(validatorIndex))
  )
}

const finalizedEpoch = epochOfSlot(finalizedSlot - 1)

let epoch = Math.min(
  ...(validatorIdsToProcess.map(validatorIndex =>
        Math.max(
          db.get(`${chainId}/validator/${validatorIndex}/nextEpoch`) ?? 0,
          validatorIdsToConsider.get(validatorIndex)
        ))
     )
)

if (process.env.OVERRIDE_START_EPOCH)
  epoch = parseInt(process.env.OVERRIDE_START_EPOCH)
const finalEpoch = parseInt(process.env.OVERRIDE_FINAL_EPOCH) || finalizedEpoch

log(`Getting data for epochs ${epoch} through ${finalEpoch}`)

const getValidatorIdsToConsiderForEpoch = (epoch) => new Set(
  Array.from(validatorIdsToConsider.entries()).flatMap(
    ([id, actEp]) => actEp <= epoch ? [id] : []
  )
)

const rewardsOptionsForEpoch = (validatorIdsToConsiderForEpoch) => ({
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify(Array.from(validatorIdsToConsiderForEpoch.keys()))
})

while (epoch <= finalEpoch) {
  log(`Processing epoch ${epoch}`)

  const validatorIdsToConsiderForEpoch = getValidatorIdsToConsiderForEpoch(epoch)

  if (!validatorIdsToConsiderForEpoch.size) {
    console.warn(`${epoch} has no relevant active validators`)
    epoch += 1
    continue
  }

  const rewardsOptions = rewardsOptionsForEpoch(validatorIdsToConsiderForEpoch)

  const firstSlotInEpoch = epoch * slotsPerEpoch

  log(`Getting attestation duties for ${epoch}`)

  const attestationDutiesUrl = new URL(
    `/eth/v1/beacon/states/${firstSlotInEpoch}/committees?epoch=${epoch}`,
    beaconRpcUrl
  )
  const committees = await fetch(attestationDutiesUrl).then(async res => {
    if (res.status !== 200)
      throw new Error(`Got ${res.status} fetching attestation duties for ${epoch}: ${await res.text()}`)
    const json = await res.json()
    return json.data
  })
  for (const {index, slot, validators} of committees) {
    for (const [position, selectedIndex] of validators.entries()) {
      if (validatorIdsToConsiderForEpoch.has(selectedIndex)) {
        const attestationKey = `${chainId}/validator/${selectedIndex}/attestation/${epoch}`
        const attestation = db.get(attestationKey) || {}
        if (!('position' in attestation)) {
          log(`Adding attestation duty @ ${slot} for validator ${selectedIndex}`)
          attestation.slot = parseInt(slot)
          attestation.index = parseInt(index)
          attestation.position = position
          await db.put(attestationKey, attestation)
        }
      }
    }
  }

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
    if (validatorIdsToConsiderForEpoch.has(validatorIndex)) {
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

  log(`Getting attestations and syncs for ${epoch}`)

  let searchSlot = firstSlotInEpoch
  while (searchSlot < firstSlotInEpoch + slotsPerEpoch) {
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
      for (const validatorIndex of validatorIdsToConsiderForEpoch.keys()) {
        const attestationKey = `${chainId}/validator/${validatorIndex}/attestation/${attestationEpoch}`
        const attestation = db.get(attestationKey)
        if (attestation?.slot == slot && attestation.index == index && !(attestation.attested?.slot < searchSlot)) {
          if (attestedBits[attestation.position]) {
            attestation.attested = { slot: searchSlot, head: beacon_block_root, source, target }
            log(`Adding attestation for ${slot} (${attestationEpoch}) for validator ${validatorIndex} ${JSON.stringify(attestation.attested)}`)
            await db.put(attestationKey, attestation)
          }
        }
      }
    }
    if (blockData.sync_aggregate) {
      const syncBits = hexStringToBitvector(blockData.sync_aggregate.sync_committee_bits)
      for (const validatorIndex of validatorIdsToConsiderForEpoch.keys()) {
        const syncKey = `${chainId}/validator/${validatorIndex}/sync/${epoch}`
        const sync = db.get(syncKey)
        if (sync) {
          if (!syncBits[sync.position] && !sync.missed.includes(searchSlot)) {
            sync.missed.push(searchSlot)
            await db.put(syncKey, sync)
          }
          else {
            log(`Adding sync message for ${searchSlot} for validator ${validatorIndex}`)
          }
        }
      }

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
          log(`Adding sync reward for ${searchSlot} for validator ${validator_index}: ${reward}`)
          sync.rewards.push({slot: searchSlot, reward})
          await db.put(syncKey, sync)
        }
      }
    }

    searchSlot++
  }

  log(`Getting attestation rewards for ${epoch}`)

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
      log(`Adding attestation reward for epoch ${epoch} for validator ${validator_index}: ${Object.entries(attestation.reward)} / ${Object.entries(attestation.ideal)}`)
      await db.put(attestationKey, attestation)
    }
  }

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
    if (validatorIdsToConsiderForEpoch.has(validator_index)) {
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

  epoch += 1
  // TODO: store multiple ranges of collected epochs per validator?
  if (!process.env.OVERRIDE_START_EPOCH) {
    const updated = []
    for (const validatorIndex of validatorIdsToConsiderForEpoch.keys()) {
      const nextEpochKey = `${chainId}/validator/${validatorIndex}/nextEpoch`
      if ((db.get(nextEpochKey) || 0) < epoch) {
        updated.push(validatorIndex)
        await db.put(nextEpochKey, epoch)
      }
    }
    if (updated.length)
      log(`Updated nextEpoch to ${epoch} for ${updated}`)
  }
}

// TODO: add listener for new epochs

await db.close()
