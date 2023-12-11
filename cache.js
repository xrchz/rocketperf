import 'dotenv/config'
import { ethers } from 'ethers'
import { open } from 'lmdb'
import { timeSlotConvs, slotsPerEpoch } from './lib.js'

const db = open({path: 'db', encoder: {structuredClone: true}})
const provider = new ethers.JsonRpcProvider(process.env.RPC)
const beaconRpcUrl = process.env.BN
const chainId = await provider.getNetwork().then(n => n.chainId)
const {timeToSlot, slotToTime} = timeSlotConvs(chainId)

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
const finalizedSlot = await fetch(
  new URL('/eth/v1/beacon/blinded_blocks/finalized', beaconRpcUrl)
).then(res => res.json().then(j => j.data.message.slot))
// TODO: only allow up to finalizedSlot in frontend

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

const validatorIdsToProcess = [918040, 509644]
const validatorIdsToConsider = new Set(validatorIdsToProcess) // TODO: get everything already in the db

const finalizedEpoch = Math.floor((finalizedSlot - 1) / slotsPerEpoch)

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
let epoch = Math.floor((nextSlot - slotsPerEpoch) / slotsPerEpoch)

console.log(`Would try to get data for epochs ${epoch} through ${finalizedEpoch}`)

if (false) {

while (epoch <= finalizedEpoch) {
  // attestation duties
  const attestationDutiesUrl = new URL(
    `/eth/v1/beacon/states/finalized/committees?epoch=${epoch}`,
    beaconRpcUrl
  )
  const committees = await fetch(attestationDutiesUrl).then(res => res.json().then(j => j.data))
  for (const {index, slot, validators} of committees) {
    for (const selectedIndex of validators) {
      if (validatorIdsToConsider.has(parseInt(selectedIndex))) {
        const attestationKey = `${chainId}/validator/${selectedIndex}/attestations/${slot}`
        const attestation = db.get(attestationKey) || {}
        if (!('committeeIndex' in attestation)) {
          attestation.committeeIndex = index
          await db.put(attestationKey, attestation)
        }
      }
    }
  }

  // TODO: attestations

  // TODO: proposal duties

  // TODO: proposals

  // TODO: sync duties

  // TODO: sync messages

  epoch += 1
  nextSlot += slotsPerEpoch
  for (const validatorIndex of validatorIdsToConsider) {
    const nextSlotKey = `${chainId}/validator/${validatorIndex}/nextSlot`
    if ((db.get(nextSlotKey) || 0) < nextSlot)
      await db.put(nextSlotKey, nextSlot)
  }
}

}

await db.close()
