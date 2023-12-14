import { open } from 'lmdb'
import { ethers } from 'ethers'

const timestamp = () => Intl.DateTimeFormat(
    'en-GB', {dateStyle: 'medium', timeStyle: 'medium'}
  ).format(new Date())

export const log = s => console.log(`${timestamp()} ${s}`)

export const provider = new ethers.JsonRpcProvider(process.env.RPC)
export const chainId = await provider.getNetwork().then(n => n.chainId)
export const beaconRpcUrl = process.env.BN

export const db = open({path: 'db', encoder: {structuredClone: true}})

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

export const getFinalizedSlot = () =>
  fetch(
    new URL('/eth/v1/beacon/blinded_blocks/finalized', beaconRpcUrl)
  ).then(res => res.json().then(j => j.data.message.slot))
