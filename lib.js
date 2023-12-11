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

