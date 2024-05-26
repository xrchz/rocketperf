import { dbFor, chainId, log, epochFromActivationInfo } from './lib.js'
import { createWriteStream } from 'node:fs'

let keys = 0
const dbvis = [1, 100000, 200000, 250000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000, 1100000, 1200000, 1300000, 1400000]

const brokenData = createWriteStream('brokenData.txt')
async function addLine(line) {
  if (brokenData.pending)
    await new Promise(resolve => brokenData.once('ready', resolve))
  if (brokenData.writableNeedDrain)
    await new Promise(resolve => brokenData.once('drain', resolve))
  brokenData.write(line)
}

for (const dbvi of dbvis) {
  const dbv = dbFor([chainId, 'validator', dbvi])
  for (const key of dbv.getKeys()) {
    if (++keys % 100000 == 0) log(`Up to key ${key}`)
    const [validatorIndex, attestationLit, epoch] = key
    if (attestationLit === 'attestation') {
      const activationInfo = dbv.get([validatorIndex,'activationInfo'])
      const activationEpoch = activationInfo && epochFromActivationInfo(activationInfo)
      if (!activationEpoch) throw new Error(`No activationEpoch for ${validatorIndex}`)
      const nextEpoch = dbv.get([validatorIndex,'nextEpoch'])
      if (!nextEpoch || nextEpoch <= parseInt(epoch)) continue
      if (parseInt(epoch) < activationEpoch) {
        await addLine(`${validatorIndex}: ${activationEpoch} > ${epoch}\n`)
        continue
      }
      const attestation = dbv.get(key)
      if (attestation?.attested && !('reward' in attestation) || !('ideal' in attestation)) {
        await addLine(`${validatorIndex}: ${epoch}\n`)
      }
    }
  }
}
brokenData.end()
