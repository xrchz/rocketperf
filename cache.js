import 'dotenv/config'
import { ethers } from 'ethers'
import { open } from 'lmdb'

const db = open({path: 'db', encoder: {structuredClone: true}})
const provider = new ethers.JsonRpcProvider(process.env.RPC)
const beaconRpcUrl = process.env.BN
const chainId = await provider.getNetwork().then(n => n.chainId)

const MAX_QUERY_RANGE = 1000

const rocketStorage = new ethers.Contract(
  await provider.resolveName('rocketstorage.eth'),
  ['event NodeWithdrawalAddressSet (address indexed node, address indexed withdrawalAddress, uint256 time)'],
  provider
)

const rocketStorageGenesisBlockByChain = {
  1: 13325233
}

const rocketStorageGenesisBlock = rocketStorageGenesisBlockByChain[chainId]

let withdrawalAddressBlock = db.get(`${chainId}/withdrawalAddressBlock`)
if (!withdrawalAddressBlock) withdrawalAddressBlock = rocketStorageGenesisBlock

const currentBlock = await provider.getBlockNumber()
while (withdrawalAddressBlock < currentBlock) {
  const min = withdrawalAddressBlock
  const max = Math.min(withdrawalAddressBlock + MAX_QUERY_RANGE, currentBlock)
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

// TODO: addListener

await db.close()
