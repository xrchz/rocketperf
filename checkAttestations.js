import { db, slotsPerEpoch, beaconRpcUrl, chainId } from './lib.js'

const start = [chainId,'validator',314965,'attestation',108713]

const cleanupThenError = async (s) => { throw new Error(s) }

let count = 0
for (const key of db.getKeys({start})) {
  if (++count % 10000 == 0) console.log(`Up to key ${key}`)
  const [chainIdLit, validatorLit, index, attestationLit, epoch] = key
  if (chainIdLit == chainId && validatorLit === 'validator' && attestationLit === 'attestation') {
    const nextEpoch = db.get([chainId,'validator',index,'nextEpoch'])
    if (!nextEpoch || nextEpoch <= parseInt(epoch)) continue
    const attestation = db.get(key)
    if (!attestation.attested)
      continue // console.warn(`${index} (${nextEpoch}) missed attestation for ${attestation.slot} (${epoch})`)
    else if (!('reward' in attestation) || !('ideal' in attestation)) {
      console.error(`${index} attested for ${attestation.slot} in ${attestation.attested.slot} but rewards not recorded, fixing...`)
      console.log(JSON.stringify(attestation))
      const firstSlotInEpoch = parseInt(epoch) * slotsPerEpoch
      const attestationRewardsUrl = new URL(
        `${beaconRpcUrl}/eth/v1/beacon/rewards/attestations/${epoch}`
      )
      const postOptions = {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify([index])
      }
      const attestationRewards = await fetch(attestationRewardsUrl, postOptions).then(async res => {
        if (res.status !== 200)
          await cleanupThenError(`Got ${res.status} fetching ${attestationRewardsUrl}: ${await res.text()}`)
        const json = await res.json()
        return json.data
      })
      const effectiveBalancesUrl = new URL(
        `${beaconRpcUrl}/eth/v1/beacon/states/${firstSlotInEpoch}/validators`
      )
      const wrappedPostOptions = {...postOptions, body: JSON.stringify({ids: [index]})}
      const effectiveBalances = await fetch(effectiveBalancesUrl, wrappedPostOptions).then(async res => {
        if (res.status !== 200)
          await cleanupThenError(`Got ${res.status} fetching ${effectiveBalancesUrl}: ${await res.text()}`)
        const json = await res.json()
        return new Map(json.data.map(({index, validator: {effective_balance}}) => [index, effective_balance]))
      })
      const effectiveBalance = effectiveBalances.get(index)
      const {validator_index, head, target, source, inactivity} = attestationRewards.total_rewards[0]
      if (validator_index != index) await cleanupThenError(`Wrong total_rewards`)
      attestation.reward = {head, target, source, inactivity}
      const ideal = attestationRewards.ideal_rewards.find(x => x.effective_balance === effectiveBalance)
      if (!ideal) await cleanupThenError(`Could not get ideal rewards for ${firstSlotInEpoch} ${index} with ${effectiveBalance}`)
      attestation.ideal = {}
      for (const key of Object.keys(attestation.reward))
        attestation.ideal[key] = ideal[key]
      console.log(`Adding attestation reward @ ${key}: ${Object.entries(attestation.reward)} / ${Object.entries(attestation.ideal)}`)
      await db.put(key, attestation)
    }
  }
}
