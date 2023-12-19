import 'dotenv/config'
import { ethers } from 'ethers'
import { db, provider, chainId, beaconRpcUrl, log,
         timeSlotConvs, slotsPerEpoch, epochOfSlot,
         getFinalizedSlot
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

/*
minipools to handle for mockup:
0xAdADA999Db795Ba2A5a1Eb61ee32CEE9C06735Cd - 918040
0xAdaDA000C278690c88C04F87382f19EEDFbc1812 - 509644
*/

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
const finalizedSlot = 6000000 // await getFinalizedSlot() TODO

let withdrawalAddressBlock = db.get(`${chainId}/withdrawalAddressBlock`)
if (!withdrawalAddressBlock) withdrawalAddressBlock = rocketStorageGenesisBlock

while (withdrawalAddressBlock < finalizedBlockNumber) {
  const min = withdrawalAddressBlock
  const max = Math.min(withdrawalAddressBlock + MAX_QUERY_RANGE, finalizedBlockNumber)
  console.log(`Processing withdrawal addresses ${min}...${max}`)
  const logs = await rocketStorage.queryFilter('NodeWithdrawalAddressSet', min, max)
  for (const log of logs) {
    const nodeAddress = log.args[0]
    const withdrawalAddress = log.args[1]
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

async function activationEpoch(validatorIndex) {
  const path = `/eth/v1/beacon/states/finalized/validators/${validatorIndex}`
  const url = new URL(path, beaconRpcUrl)
  const res = await fetch(url)
  const json = await res.json()
  if (!(0 <= parseInt(json?.data?.validator?.activation_epoch)))
    throw new Error(`Failed to get activation_epoch for ${validatorIndex}`)
  return parseInt(json.data.validator.activation_epoch)
}

const validatorIdsToProcess = ['918040', '509644']

// TODO: get everything already in the db + validatorIdsToProcess
const validatorIdsToConsider = new Map()
for (const validatorIndex of validatorIdsToProcess) {
  validatorIdsToConsider.set(validatorIndex, await activationEpoch(validatorIndex))
}

const finalizedEpoch = epochOfSlot(finalizedSlot - 1)

let epoch = Math.min(
  ...(validatorIdsToProcess.map(validatorIndex =>
        db.get(`${chainId}/validator/${validatorIndex}/nextEpoch`) ??
        validatorIdsToConsider.get(validatorIndex))
     )
)

log(`Getting data for epochs ${epoch} through ${finalizedEpoch}`)

const rewardsOptionsForEpoch = (epoch) => ({
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify(
    Array.from(validatorIdsToConsider.entries()).flatMap(
      ([id, actEp]) => actEp <= epoch ? [id] : []
    )
  )
})

while (epoch <= finalizedEpoch) {
  log(`Processing epoch ${epoch}`)

  const rewardsOptions = rewardsOptionsForEpoch(epoch)

  if (rewardsOptions.body === '[]')
    throw new Error(`${epoch} has no relevant active validators`)

  const firstSlotInEpoch = epoch * slotsPerEpoch

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
      if (validatorIdsToConsider.has(selectedIndex)) {
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

  const syncDutiesUrl = new URL(
    `/eth/v1/beacon/states/${firstSlotInEpoch}/sync_committees?epoch=${epoch}`,
    beaconRpcUrl
  )
  const syncValidators = await fetch(syncDutiesUrl).then(async res => {
    if (res.status !== 200)
      throw new Error(`Got ${res.status} fetching sync duties for ${epoch}: ${await res.text()}`)
    const json = await res.json()
    return json.data.validators
  })
  for (const [position, validatorIndex] of syncValidators.entries()) {
    if (validatorIdsToConsider.has(validatorIndex)) {
      const syncKey = `${chainId}/validator/${validatorIndex}/sync/${epoch}`
      const sync = db.get(syncKey) || {}
      if (!('position' in sync)) {
        log(`Adding sync duty for epoch ${epoch} for validator ${selectedIndex}`)
        sync.position = position
        sync.missed = []
        sync.rewards = []
        await db.put(syncKey, sync)
      }
    }
  }

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
    for (const {aggregation_bits, data: {slot, index}} of attestations) {
      const attestedBits = hexStringToBitlist(aggregation_bits)
      const attestationEpoch = epochOfSlot(parseInt(slot))
      for (const validatorIndex of validatorIdsToConsider.keys()) {
        const attestationKey = `${chainId}/validator/${validatorIndex}/attestation/${attestationEpoch}`
        const attestation = db.get(attestationKey)
        if (attestation?.slot == slot && attestion.index == index && !attestation.attested) {
          if (attestedBits[attestation.position]) {
            log(`Adding attestation for ${slot} for validator ${validatorIndex} (found in ${searchSlot})`)
            attestation.attested = true
            await db.put(attestationKey, attestation)
          }
        }
      }
    }
    if (blockData.sync_aggregate) {
      const syncBits = hexStringToBitvector(blockData.sync_aggregate.sync_committee_bits)
      for (const validatorIndex of validatorIdsToConsider.keys()) {
        const syncKey = `${chainId}/validator/${validatorIndex}/sync/${epoch}`
        const sync = db.get(syncKey)
        if (sync) {
          if (!syncBits[sync.position]) {
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
        log(`Adding sync reward for ${searchSlot} for validator ${validator_index}`)
        sync.rewards.push(reward)
        await db.push(syncKey)
      }
    }

    searchSlot++
  }

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
    if (attestation && !('reward' in attestation)) {
      log(`Adding attestation reward for epoch ${epoch} for validator ${validator_index}`)
      attestation.reward = {head, target, source, inactivity}
      await db.put(attestationKey, attestation)
    }
  }

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
    if (validatorIdsToConsider.has(validator_index)) {
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
  for (const validatorIndex of validatorIdsToConsider.keys()) {
    const nextEpochKey = `${chainId}/validator/${validatorIndex}/nextEpoch`
    if ((db.get(nextEpochKey) || 0) < epoch) {
      log(`Updating nextEpoch to ${epoch} for ${validatorIndex}`)
      await db.put(nextEpochKey, epoch)
    }
  }
}

// TODO: add listener for new epochs

await db.close()
