import { open } from 'lmdb'
import { readFileSync } from 'node:fs'
const oldDbPath = '/home/ramana/lh/db/1/200000-299999'
const newDbPath = '/home/ramana/bf3d/tmp/250000-299999'
const oldDb = open({path: oldDbPath})
const newDb = open({path: newDbPath})
const chainId = 1
/*
const startValidator = parseInt(process.env.START)
const endValidator = parseInt(process.env.END)
if (!(0 <= startValidator && startValidator < endValidator))
  throw new Error(`Bad START and END`)
const start = [chainId, 'validator', startValidator]
const end = [chainId, 'validator', endValidator]
*/
/*
const start = [chainId, 'w']
// const end = [chainId, 'validator', '']
*/
// Up to 1,validator,271871,attestation,219546, skipped 300000000 already existing...
// Up to 1,validator,275213,attestation,117673, skipped 43600000 already existing...
const start = [250000]
const end = undefined
let existCount = 0n
const batchSize = parseInt(process.env.BS) || 8192
const batch = []
async function clearBatch() {
  const {key: lastKey} = batch.at(-1)
  await newDb.transaction(() => {
    for (const {newKey, newValue} of batch.splice(0, Infinity)) {
      newDb.put(newKey, newValue)
    }
  })
  console.log(`Copied up to ${lastKey}`)
}
for (const {key, value} of oldDb.getRange({start, end})) {
  let newKey
  let newValue = value
  if (key.length >= 3 && key[1] == 'validator')
    newKey = `${key} should be sharded`
  else
    newKey = key
  if (!(key instanceof Array)) {
    throw new Error(`Unexpected non-array key ${key}`)
    /*
    newKey = key.split('/')
    if (newKey[0] != chainId) throw new Error(`Unexpected chainId ${newKey[0]}`)
    newKey[0] = parseInt(chainId)
    for (let i = 1; i < newKey.length; i++) {
      if (newKey[i] === 'validator' || newKey[i] === 'attestation' || newKey[i] === 'sync' || newKey[i] === 'proposal')
        newKey[i+1] = parseInt(newKey[++i])
      else if ((newKey[i] === 'nextEpoch' || newKey[i] === 'dutiesEpoch') && i+1 < newKey.length)
        newKey[i+1] = parseInt(newKey[++i])
    }
    */
  }
  if (newDb.doesExist(newKey)) {
    if (++existCount % 100000n == 0n)
      console.log(`Up to ${key}, skipped ${existCount} already existing...`)
    continue
  }
  /*
  if (value instanceof Set) {
    console.log(`Replacing Set for ${key}`)
    newValue = Array.from(value.keys())
  }
  if (value instanceof Map) {
    console.log(`Replacing Map for ${key}`)
    newValue = {}
    for (const [k, v] of value.entries()) {
      newValue[k] = v
    }
  }
  */
  batch.push({key, newKey, newValue, value})
  if (batch.length >= batchSize) await clearBatch()
}
if (batch.length) await clearBatch()
