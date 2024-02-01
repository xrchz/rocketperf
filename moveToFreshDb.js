import { open } from 'lmdb'
import { readFileSync } from 'node:fs'
const oldDbPath = '/home/ramana/lh/dbold'
const newDbPath = '/home/ramana/lh/db'
const oldDb = open({path: oldDbPath})
const newDb = open({path: newDbPath})
const chainId = 1
const startValidator = parseInt(process.env.START)
const endValidator = parseInt(process.env.END)
if (!(0 <= startValidator && startValidator < endValidator))
  throw new Error(`Bad START and END`)
const start = [chainId, 'validator', startValidator]
const end = [chainId, 'validator', endValidator]
/*
const start = [chainId, 'w']
// const end = [chainId, 'validator', '']
const end = undefined
*/
const batchSize = parseInt(process.env.BS) || 8192
const batch = []
for (const {key, value} of oldDb.getRange({start, end})) {
  if (newDb.doesExist(key)) {
    console.warn(`${key} already exists in ${newDbPath}: skipping`)
    continue
  }
  batch.push({key, value})
  if (batch.length >= batchSize) {
    const {key: lastKey} = batch.at(-1)
    await newDb.transaction(() => {
      for (const {key, value} of batch.splice(0, Infinity)) {
        newDb.put(key, value)
      }
    })
    console.log(`Replaced up to ${lastKey}`)
  }
}
