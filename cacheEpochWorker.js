import { parentPort } from 'node:worker_threads'
import { db, log, chainId, slotsPerEpoch, epochOfSlot, beaconRpcUrl } from './lib.js'

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

const rewardsOptionsForEpoch = (validatorIds) => ({
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify(Array.from(validatorIds.keys()))
})

async function processEpoch(epoch, validatorIds) {
  log(`Processing epoch ${epoch}`)

  if (!validatorIds.size) {
    console.warn(`${epoch} has no relevant active validators`)
    return
  }

  const rewardsOptions = rewardsOptionsForEpoch(validatorIds)

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
      if (validatorIds.has(selectedIndex)) {
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
      for (const validatorIndex of validatorIds.keys()) {
        const attestationKey = `${chainId}/validator/${validatorIndex}/attestation/${attestationEpoch}`
        const attestation = db.get(attestationKey)
        if (attestation?.slot == slot && attestation.index == index && !(attestation.attested?.slot <= searchSlot)) {
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
      for (const validatorIndex of validatorIds.keys()) {
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

parentPort.on('message', async ({epoch, validatorIds}) => {
  await processEpoch(epoch, validatorIds)
  parentPort.postMessage('done')
})
