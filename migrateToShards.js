import { chainId, dbFor } from './lib.js'
const db = dbFor([])
const startIndex = parseInt(process.env.START) || null
const endIndex = parseInt(process.env.END) || ''
const start = [chainId,'validator',startIndex]
const end = [chainId,'validator',endIndex]
let existCount = 0n
const batchSize = parseInt(process.env.BS) || 8192
const deleteBatch = []
const insertBatches = new Map()
async function clearBatches(force) {
  const insertPromises = []
  const deletePromises = []
  for (const [target, insertBatch] of insertBatches.entries()) {
    const {newKey: lastNewKey} = insertBatch.at(-1) || {}
    if (insertBatch.length >= batchSize || (force && insertBatch.length)) {
      const toInsert = insertBatch.splice(0, Infinity)
      insertPromises.push(
        target.transaction(() => {
          for (const {newKey, value} of toInsert) target.put(newKey, value)
        }).then(() => deleteBatch.push(...toInsert.map(({oldKey}) => oldKey)))
      )
    }
    if (insertPromises.length) console.log(`Inserting up to ${lastNewKey}`)
  }
  await Promise.all(insertPromises)
  const lastOldKey = deleteBatch.at(-1)
  if (deleteBatch.length >= batchSize || (force && deleteBatch.length)) {
    deletePromises.push(
      db.transaction(() => {
        for (const oldKey of deleteBatch.splice(0, Infinity))
          db.remove(oldKey)
      })
    )
  }
  if (deletePromises.length) {
    console.log(`Deleting up to ${lastOldKey}`)
    await Promise.all(deletePromises)
  }
}

for (const {key, value} of db.getRange({start, end})) {
  if (key.length < 3 || key[1] !== 'validator')
    throw new Error(`Unexpected key ${key}`)
  const target = dbFor(key)
  if (!insertBatches.has(target)) insertBatches.set(target, [])
  const newKey = key.slice(2)
  if (target.doesExist(newKey))
    throw new Error(`${newKey} already present in target`)
  insertBatches.get(target).push({oldKey: key, newKey, value})
  await clearBatches()
}
await clearBatches(true)
