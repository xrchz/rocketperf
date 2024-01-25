import 'dotenv/config'
import { open } from 'lmdb'
import { ethers } from 'ethers'

const timestamp = () => Intl.DateTimeFormat(
    'en-GB', {dateStyle: 'medium', timeStyle: 'medium'}
  ).format(new Date())

export const log = s => console.log(`${timestamp()} ${s}`)

const MAX_ARGS = 10000

export const arrayMin = (a) => {
  let min = Infinity
  while (a.length) min = Math.min(min, ...a.splice(0, MAX_ARGS))
  return min
}

export const arrayMax = (a) => {
  let max = 0
  while (a.length) max = Math.max(max, ...a.splice(0, MAX_ARGS))
  return max
}

export const filterResolved = (a) => {
  let i = 0
  for (const {state, task} of a) {
    if (!state.resolved)
      a[i++] = {state, task}
  }
  a.length = i
}

const moreTasksLocked = new Uint8Array(1)
const moreTasksPending = []
const moreTasks = []

export const finishMoreTasks = () => Promise.allSettled(moreTasks.concat(moreTasksPending))

let runningArrayPromises = true
export const interruptArrayPromises = () => runningArrayPromises = false

export const arrayPromises = async (a, max, logger) => {
  if (logger) log(`Processing ${a.length} promises ${max} at a time`)
  const result = []
  while (runningArrayPromises && a.length) {
    if (logger) logger(a.length)
    const batch = a.splice(0, max).map(f => {
      const state = {}
      return {state, task: f().finally(() => state.resolved = true)}
    })
    moreTasksPending.push(...batch)
    result.push(...await Promise.all(batch.map(({task}) => task)))
  }
  if (Atomics.compareExchange(moreTasksLocked, 0, 0, 1) === 0) {
    moreTasks.push(...moreTasksPending.splice(0, Infinity))
    filterResolved(moreTasks)
    Atomics.store(moreTasksLocked, 0, 0)
  }
  return result
}

export const nullAddress = '0x'.padEnd(42, '0')

export const provider = new ethers.JsonRpcProvider(process.env.RPC)
export const chainId = await provider.getNetwork().then(n => n.chainId)
export const beaconRpcUrl = process.env.BN

export const db = open({path: 'db', encoder: {structuredClone: true}})

export const FAR_FUTURE_EPOCH = 2 ** 64 - 1
export const secondsPerSlot = 12
export const slotsPerEpoch = 32
export const genesisTimes = {
  1: 1606824023
}
export const timeSlotConvs = (chainId) => {
  const genesisTime = genesisTimes[chainId]
  return {
    timeToSlot: (t) => Math.round((t - genesisTime) / secondsPerSlot),
    slotToTime: (s) => genesisTime + s * secondsPerSlot
  }
}

export const epochOfSlot = (slot) => Math.floor(slot / slotsPerEpoch)

export const getFinalizedSlot = () =>
  fetch(
    new URL(`${beaconRpcUrl}/eth/v1/beacon/blinded_blocks/finalized`)
  ).then(res => res.json().then(j => j.data.message.slot))

export async function getIndexFromPubkey(pubkey) {
  const key = `${chainId}/validatorIndex/${pubkey}`
  const cached = db.get(key)
  if (isNaN(parseInt(cached))) {
    const path = `/eth/v1/beacon/states/finalized/validators/${pubkey}`
    const url = new URL(`${beaconRpcUrl}${path}`)
    const response = await fetch(url)
    if (response.status === 404) {
      log(`Validator index missing for pubkey ${pubkey}`)
      return -1
    }
    if (response.status !== 200) {
      console.warn(`Unexpected response status getting ${pubkey} index: ${response.status}`)
      return -2
    }
    const index = await response.json().then(j => j.data.index)
    await db.put(key, index)
    return index
  } else return cached
}

export async function getPubkeyFromIndex(index) {
  const path = `/eth/v1/beacon/states/finalized/validators/${index}`
  const url = new URL(`${beaconRpcUrl}${path}`)
  const response = await fetch(url)
  if (response.status === 404)
    return null
  if (response.status !== 200)
    console.warn(`Unexpected response status getting ${index} pubkey: ${response.status}`)
  return await response.json().then(j => j.data.validator.pubkey)
}

export const rocketStorage = new ethers.Contract(
  await provider.resolveName('rocketstorage.eth'),
  ['function getAddress(bytes32 _key) view returns (address)'],
  provider
)
log(`Rocket Storage: ${await rocketStorage.getAddress()}`)

export const getRocketAddress = name =>
  rocketStorage['getAddress(bytes32)'](ethers.id(`contract.address${name}`))

export const rocketMinipoolManager = new ethers.Contract(
  await getRocketAddress('rocketMinipoolManager'), [
    'function getMinipoolExists(address) view returns (bool)',
    'function getMinipoolPubkey(address) view returns (bytes)',
    'function getMinipoolCount() view returns (uint256)',
    'function getMinipoolAt(uint256) view returns (address)',
    'function getNodeMinipoolCount(address) view returns (uint256)',
    'function getNodeMinipoolAt(address, uint256) view returns (address)'
  ], provider)
log(`Minipool Manager: ${await rocketMinipoolManager.getAddress()}`)

export const minipoolAbi = [
  'function getNodeAddress() view returns (address)',
  'function getVacant() view returns (bool)',
  'function getStatus() view returns (uint256)',
  'event MinipoolPromoted(uint256 time)'
]

const multicallContract = new ethers.Contract(
  '0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441',
  ['function aggregate((address,bytes)[]) view returns (uint256, bytes[])'],
  provider)

export function multicall(calls) {
  log(`entered multicall with ${calls.length} calls`)
  const margs = []
  const posts = []
  for (const {contract, fn, args} of calls) {
    const iface = contract.interface
    const ff = iface.getFunction(fn)
    const data = iface.encodeFunctionData(ff, args)
    margs.push(contract.getAddress().then(addr => [addr, data]))
    posts.push(res => iface.decodeFunctionResult(ff, res)[0])
  }
  return Promise.all(margs)
    .then(calls => multicallContract.aggregate(calls))
    .then(res => Array.from(res[1]).map((r, i) => posts[i](r)))
}

export let minipoolsByPubkeyCount = db.get(`${chainId}/minipoolsByPubkeyCount`) ?? 0
export const incrementMinipoolsByPubkeyCount = n => minipoolsByPubkeyCount += n
export const minipoolsByPubkey = db.get(`${chainId}/minipoolsByPubkey`) ?? new Map()
export let minipoolCount
export const updateMinipoolCount = async () => {
  minipoolCount = parseInt(
    await rocketMinipoolManager.getMinipoolCount({blockTag: 'finalized'})
  )
}
export const getMinipoolByPubkey = pubkey =>
  minipoolsByPubkey.get(pubkey) ?? nullAddress

export const epochFromActivationInfo = activationInfo =>
  activationInfo.promoted ?
    epochOfSlot(activationInfo.promoted) :
    activationInfo.beacon
