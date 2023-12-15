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
/* TODO:

to store:
per chain:
per minipool validator (by validator index):
- attestation assignments (committee for slot, assigned at slot)
- per attestation assignment: miss or pass, reward/penalty, accuracy info
- proposal assignments
- per proposal assignment: miss or pass, reward/penalty
- sync assignments
- per sync duty slot: miss or pass, reward/penalty
- TODO: find out if there are accuracy details for sync
- slot range (or latest slot) for which we're up to date
- start slot for which it is a minipool validator? TODO: also pay attention to this in the frontend
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
const finalizedSlot = await getFinalizedSlot()

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

const validatorSlot = db.get(`${chainId}/validatorSlot`) || new Map()

const validatorIdsToProcess = ['918040', '509644']
const validatorIdsToConsider = new Set(validatorIdsToProcess) // TODO: get everything already in the db

const finalizedEpoch = epochOfSlot(finalizedSlot - 1)

async function activationEpoch(validatorIndex) {
  const path = `/eth/v1/beacon/states/finalized/validators/${validatorIndex}`
  const url = new URL(path, beaconRpcUrl)
  const res = await fetch(url)
  const json = await res.json()
  if (!(0 <= parseInt(json?.data?.validator?.activation_epoch))) {
    console.error(`Failed to get activation_epoch for ${validatorIndex}`)
    process.exit(1)
  }
  return parseInt(json.data.validator.activation_epoch)
}

let nextSlot = Math.min(
  ...(await Promise.all(
        validatorIdsToProcess.map(async validatorIndex =>
          (db.get(`${chainId}/validator/${validatorIndex}/nextSlot`) ??
           await activationEpoch(validatorIndex).then(epoch => epoch * slotsPerEpoch))
        )))
)
let epoch = epochOfSlot(nextSlot - slotsPerEpoch)

log(`Getting data for epochs ${epoch} through ${finalizedEpoch}`)

const rewardsOptions = {
  method: 'POST',
  body: JSON.stringify(Array.from(validatorIdsToConsider.values()))
}

// TODO: add more logging and more checking of e.g. response statuses

while (epoch <= finalizedEpoch) {
  const attestationDutiesUrl = new URL(
    `/eth/v1/beacon/states/finalized/committees?epoch=${epoch}`,
    beaconRpcUrl
  )
  const committees = await fetch(attestationDutiesUrl).then(res => res.json().then(j => j.data))
  for (const {index, slot, validators} of committees) {
    for (const [position, selectedIndex] of validators.entries()) {
      if (validatorIdsToConsider.has(selectedIndex)) {
        const attestationKey = `${chainId}/validator/${selectedIndex}/attestation/${epoch}`
        const attestation = db.get(attestationKey) || {}
        if (!('position' in attestation)) {
          attestation.slot = parseInt(slot)
          attestation.index = parseInt(index)
          attestation.position = position
          await db.put(attestationKey, attestation)
        }
      }
    }
  }

  const syncDutiesUrl = new URL(
    `/eth/v1/beacon/states/finalized/sync_committees?epoch=${epoch}`,
    beaconRpcUrl
  )
  const syncValidators = await fetch(syncDutiesUrl).then(res => res.json().then(j => j.data.validators))
  for (const [position, validatorIndex] of syncValidators.entries()) {
    if (validatorIdsToConsider.has(validatorIndex)) {
      const syncKey = `${chainId}/validator/${validatorIndex}/sync/${epoch}`
      const sync = db.get(syncKey) || {}
      if (!('position' in sync)) {
        sync.position = position
        sync.missed = []
        sync.rewards = []
        await db.put(syncKey, sync)
      }
    }
  }

  let searchSlot = nextSlot
  while (searchSlot < nextSlot + slotsPerEpoch) {
    const blockUrl = new URL(
      `/eth/v1/beacon/blinded_blocks/${searchSlot}`,
      beaconRpcUrl
    )
    const blockData = await fetch(blockUrl).then(res => res.json().then(j => j.data.message.body))
    const attestations = blockData.attestations
    for (const {aggregation_bits, data: {slot, index}} of attestations) {
      const attestedBits = hexStringToBitlist(aggregation_bits)
      const attestationEpoch = epochOfSlot(parseInt(slot))
      for (const validatorIndex of validatorIdsToConsider.values()) {
        const attestationKey = `${chainId}/validator/${validatorIndex}/attestation/${attestationEpoch}`
        const attestation = db.get(attestationKey)
        if (attestation?.slot === slot && attestion.index === index && !attestation.attested) {
          if (attestedBits[attestation.position]) {
            attestation.attested = true
            await db.put(attestationKey, attestation)
          }
        }
      }
    }
    const syncBits = hexStringToBitvector(blockData.sync_aggregate.sync_committee_bits)
    for (const validatorIndex of validatorIdsToConsider.values()) {
      const syncKey = `${chainId}/validator/${validatorIndex}/sync/${epoch}`
      const sync = db.get(syncKey)
      if (sync) {
        if (!syncBits[sync.position]) {
          sync.missed.push(searchSlot)
          await db.put(syncKey, sync)
        }
      }
    }

    const syncRewardsUrl = new URL(
      `/eth/v1/beacon/rewards/sync_committee/${searchSlot}`,
      beaconRpcUrl
    )
    const syncRewards = await fetch(syncRewardsUrl, rewardsOptions).then(res => res.json().then(j => j.data))
    for (const {validator_index, reward} of syncRewards) {
      const syncKey = `${chainId}/validator/${validator_index}/sync/${epoch}`
      const sync = db.get(syncKey)
      sync.rewards.push(reward)
      await db.push(syncKey)
    }

    searchSlot++
  }

  const attestationRewardsUrl = new URL(
    `/eth/v1/beacon/rewards/attestations/${epoch}`,
    beaconRpcUrl
  )
  const attestationRewards = await fetch(attestationRewardsUrl, rewardsOptions).then(res =>
    res.json().then(j => j.data))
  for (const {validator_index, head, target, source, inactivity} of attestationRewards.total_rewards) {
    const attestationKey = `${chainId}/validator/${validator_index}/attestation/${epoch}`
    const attestation = db.get(attestationKey)
    if (attestation && !('reward' in attestation)) {
      attestation.reward = {head, target, source, inactivity}
      await db.put(attestationKey, attestation)
    }
  }

  const proposalUrl = new URL(
    `/eth/v1/validator/duties/proposer/${epoch}`,
    beaconRpcUrl
  )
  const proposals = await fetch(proposalUrl).then(res => res.json().then(j => j.data))
  for (const {validator_index, slot} of proposals) {
    if (validatorIdsToConsider.has(validator_index)) {
      const proposalKey = `${chainId}/validator/${validator_index}/proposal/${slot}`
      const proposal = db.get(proposalKey) || {}
      if (!('reward' in proposal)) {
        const proposalRewardUrl = new URL(`/eth/v1/beacon/rewards/blocks/${slot}`, beaconRpcUrl)
        const response = await fetch(proposalRewardUrl)
        if (response.status === 404)
          proposal.reward = '0'
        else {
          const reward = await response.json().then(j => j.data.total)
          proposal.reward = reward
        }
        await db.put(proposalKey, proposal)
      }
    }
  }

  epoch += 1
  nextSlot += slotsPerEpoch
  for (const validatorIndex of validatorIdsToConsider) {
    const nextSlotKey = `${chainId}/validator/${validatorIndex}/nextSlot`
    if ((db.get(nextSlotKey) || 0) < nextSlot)
      await db.put(nextSlotKey, nextSlot)
  }
}

await db.close()
