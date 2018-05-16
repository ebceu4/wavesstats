import { MongoClient } from 'mongodb'
import { config } from './config'
import axios from 'axios'
import { Block, TransactionType, Asset, Tx7 } from './api/interfaces'
import * as J from 'json-bigint'
import * as W from '@waves/waves-api'

const waves = W.create(W.MAINNET_CONFIG)

const cache: { [i: string]: number } = {}

async function getDecimals(asset: Asset): Promise<number> {

  if (typeof asset == 'string' && asset != null) {
    if (cache[asset] && cache[asset] > 0) {
      return cache[asset]
    }

    try {
      const d = (await axios.get(`https://nodes.wavesnodes.com/transactions/info/${asset}`)).data.decimals
      cache[asset] = d
      return d
    } catch (error) {
      return await getDecimals(asset)
    }
  }

  return 8
}

async function getBlocks(from: number, to: number): Promise<Block[]> {

  const result = (await axios.get(`https://nodes.wavesnodes.com/blocks/seq/${from}/${to}`, {
    transformResponse: (data) => {
      const blocks: any[] = J.parse(data)
      return blocks.map((b: Block) => {
        b._id = b.signature
        return b
      })
    }
  })).data

  const r = await Promise.all(result.map((b: Block) => Promise.all(
    b.transactions.filter(t => t.type == TransactionType.Exchange).map(async (t: Tx7) => {
      t.order1.sender = waves.tools.getAddressFromPublicKey(t.order1.senderPublicKey)
      t.order2.sender = waves.tools.getAddressFromPublicKey(t.order2.senderPublicKey)

      const amountAsset = t.order1.assetPair.amountAsset
      const priceAsset = t.order1.assetPair.priceAsset

      const amountDecimals = await getDecimals(amountAsset)
      const priceDecimals = await getDecimals(priceAsset)

      t.pair = { amountAsset, amountDecimals, priceAsset, priceDecimals }

      return true
    }))
  ))

  return result
}


async function fetchBlocks() {
  let blocksTotal = 0
  const db = await MongoClient.connect(config.mongoUri)
  const blocksTable = await db.db('waves').createCollection('blocks')
  const h = (await axios.get('https://nodes.wavesnodes.com/blocks/height')).data.height
  let c = h
  for (let index = 0; index < 100; index++) {
    const t = c
    c -= 9
    const blocks = await getBlocks(t - 9, t)

    Promise.all(blocks.map(block => {
      blocksTable.update(
        { "_id": block._id },
        { $set: block },
        { upsert: true }
      )
    })).then(_ => {
      blocksTotal += 10
      console.log(`Blocks saved: ${blocksTotal}`)
    })

  }

  db.close()

}

async function main() {
  await fetchBlocks()
}

main()









