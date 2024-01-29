import { db, chainId, log } from './lib.js'

const activationEpochs = new Map()

const getActivationEpoch = (validatorIndex) => {
  const cached = activationEpochs.get(validatorIndex)
  if (typeof cached != 'undefined') return cached
  const key = `${chainId}/validator/${validatorIndex}/activationInfo`
  const activationInfo = db.get(key)
  const epoch = activationInfo.beacon
  activationEpochs.set(validatorIndex, epoch)
  return epoch
}

let lastFetch = Date.now()
const minWait = 500

const fetchBeaconchain = async (path) => {
  const url = new URL(path, 'https://beaconcha.in')
  const now = Date.now()
  const toWait = lastFetch + minWait - now
  if (0 < toWait) await new Promise(resolve => setTimeout(resolve, toWait))
  const res = await fetch(url)
  lastFetch = Date.now()
  return await res.json()
}

// const numRecordsCache = new Map()
const getNumRecords = async (validatorIndex) => {
  // const cached = numRecordsCache.get(validatorIndex)
  // if (typeof cached != 'undefined') return cached
  const path = `/validator/${validatorIndex}/attestations?draw=0&start=0`
  const json = await fetchBeaconchain(path)
  const n = json.recordsTotal
  // numRecordsCache.set(validatorIndex, n)
  return n
}

const epochRe = /\/epoch\/(\d+)/
const slotRe = /\/slot\/(\d+)/
const attestedRe = /Missed|Attested/

const checkAttestationForEpoch = async (validatorIndex, epoch) => {
  const activationEpoch = await getActivationEpoch(validatorIndex)
  const numRecords = await getNumRecords(validatorIndex)
  const n = numRecords - (epoch - activationEpoch + 1)
  const path = `/validator/${validatorIndex}/attestations?draw=1&start=${n}&length=1`
  const json = await fetchBeaconchain(path)
  if (!(json.data?.length >= 1))
    throw new Error(`Got bad data for ${path}: ${JSON.stringify(json)}`)
  const item = json.data[0]
  if (!(item?.length >= 4))
    throw new Error(`Got bad item for ${path}: ${JSON.stringify(json)}`)
  const [, foundEpoch] = epochRe.exec(item[0]) || []
  if (foundEpoch != epoch)
    throw new Error(`Failed to get ${epoch} from ${path} (got ${foundEpoch} from ${item[0]})`)
  const [, slot] = slotRe.exec(item[1])
  const [attestedStr] = attestedRe.exec(item[2])
  const attested = attestedStr == 'Attested'
  const attestedSlot = attested ? slotRe.exec(item[4])[1] : undefined
  return {slot, attested, attestedSlot}
}

while (true) {
  const randomIndex = Math.floor(Math.random() * 1000000)
  const randomEpoch = Math.floor(Math.random() * 1000000)
  const start = `${chainId}/validator/${randomIndex}/attestation/${randomEpoch}`
  const end = `${chainId}/validator/${randomIndex}/attestation/a`
  for (const key of db.getKeys({start, end, snapshot: false})) {
    const [chainIdLit, validatorLit, validatorIndex, attestationLit, epochStr] = key.split('/')
    if (chainIdLit == chainId && validatorLit === 'validator' && attestationLit === 'attestation') {
      const epoch = parseInt(epochStr)
      const nextEpoch = db.get(`${chainId}/validator/${validatorIndex}/nextEpoch`)
      if (!nextEpoch || nextEpoch <= epoch) continue
      log(`Checking ${key}`)
      const attestation = db.get(key)
      const {slot, attested, attestedSlot} = await checkAttestationForEpoch(validatorIndex, epoch)
      if (!!attestation.attested != attested)
        throw new Error(`Attested discrepancy with beaconcha.in for validator ${validatorIndex} epoch ${epochStr}`)
      if (attestation.slot != slot)
        throw new Error(`Slot discrepancy with beaconcha.in for validator ${validatorIndex} epoch ${epochStr}`)
      if (attested && attestation.attested.slot != attestedSlot)
        throw new Error(`Attested slot discrepancy with beaconcha.in for validator ${validatorIndex} epoch ${epochStr}`)
      break
    }
  }
}
