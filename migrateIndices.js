import { db, chainId } from './lib.js'

const start = [chainId,'validatorIndex','0']
const end = [chainId,'validatorIndex','1']

const replacements = []
const batchSize = parseInt(process.env.BS) || 8192

for (const {key, value} of db.getRange({start, end})) {
  if (key.length != 3 || key[1] != 'validatorIndex') {
    console.log(`Skipping ${key} since it is the wrong format'`)
    continue
  }
  if (typeof value != 'string') {
    console.log(`Skipping ${value}: ${typeof value} for ${key} since it is not a string`)
    continue
  }
  const newValue = parseInt(value)
  if (typeof newValue != 'number' || isNaN(newValue)) {
    console.warn(`Unexpected NaN for ${key}: ${value} : ${typeof value}`)
    break
  }
  replacements.push({key, newValue})
  if (replacements.length >= batchSize) {
    const {key: lastKey} = replacements.at(-1)
    await db.transaction(() => {
      for (const {key, newValue} of replacements.splice(0, Infinity)) {
        db.put(key, newValue)
      }
    })
    console.log(`Replaced up to ${lastKey}`)
  }
}
