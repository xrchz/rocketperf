import { db, chainId } from './lib.js'

const start = process.env.START
const end = process.env.END

const replacements = []
const batchSize = parseInt(process.env.BS) || 8192
async function clearBatch() {
  const {key: lastKey} = replacements.at(-1)
  await db.transaction(() => {
    for (const {key, newKey, value} of replacements.splice(0, Infinity)) {
      db.remove(key)
      db.put(newKey, value)
    }
  })
  console.log(`Replaced up to ${lastKey}`)
}

for (const {key, value} of db.getRange({start, end})) {
  if (typeof key != 'string') {
    console.log(`Skipping ${key} since it is not a string`)
    continue
  }
  const newKey = key.split('/')
  if (newKey[0] != chainId) throw new Error(`Unexpected chainId ${newKey[0]}`)
  newKey[0] = parseInt(chainId)
  for (let i = 1; i < newKey.length; i++) {
    if (newKey[i] === 'validator' || newKey[i] === 'attestation' || newKey[i] === 'sync' || newKey[i] === 'proposal')
      newKey[i+1] = parseInt(newKey[++i])
    else if ((newKey[i] === 'nextEpoch' || newKey[i] === 'dutiesEpoch') && i+1 < newKey.length)
      newKey[i+1] = parseInt(newKey[++i])
  }
  replacements.push({key, newKey, value})
  console.log(`Will replace ${key} with ${newKey}`)
  if (replacements.length >= batchSize) await clearBatch()
}
if (replacements.length) await clearBatch()
