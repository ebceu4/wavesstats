import { MongoClient, Collection } from 'mongodb'
import { config } from './config'
import axios from 'axios'
import { Block, TransactionType, Asset, Tx7 } from './api/interfaces'
import * as J from 'json-bigint'
import * as W from '@waves/waves-api'
import * as progress from 'cli-progress'

const bar = new progress.Bar({}, progress.Presets.shades_classic)
const waves = W.create(W.MAINNET_CONFIG)

const assetChache: { [i: string]: number } = {}
const aliasChache: { [i: string]: string } = {}

async function getDecimals(asset: Asset): Promise<number> {

  if (typeof asset == 'string' && asset != null) {
    if (assetChache[asset] && assetChache[asset] > 0) {
      return assetChache[asset]
    }

    try {
      const d = (await axios.get(`https://nodes.wavesnodes.com/transactions/info/${asset}`)).data.decimals
      assetChache[asset] = d
      return d
    } catch (error) {
      console.log(error)
      return await getDecimals(asset)
    }
  }
  return 8
}

async function resolveAlias(alias: string): Promise<string> {
  if (aliasChache[alias] && aliasChache[alias].length > 0) {
    return aliasChache[alias]
  }
  try {
    const d = (await axios.get(`https://nodes.wavesnodes.com/alias/by-alias/${alias}`)).data.address
    aliasChache[alias] = d
    return d
  } catch (error) {
    console.log(error)
    return await resolveAlias(alias)
  }
}

async function getBlocks(from: number, to: number): Promise<Block[]> {
  const aliasPrefix = 'alias:W:'
  try {
    const result = (await axios.get(`https://nodes.wavesnodes.com/blocks/seq/${from}/${to}`, {
      transformResponse: (data) => {
        try {
          const blocks: any[] = J.parse(data)
          return blocks.map((b: Block) => {
            b._id = b.signature
            return b
          })
        } catch (ex) { console.log(ex) }
      }
    })).data
    const r = await Promise.all(result.map((b: Block) => Promise.all(
      b.transactions.map(async (t) => {
        try {
          switch (t.type) {
            case TransactionType.Exchange: {
              const tx = <any>t
              tx.order1.sender = waves.tools.getAddressFromPublicKey(tx.order1.senderPublicKey)
              tx.order2.sender = waves.tools.getAddressFromPublicKey(tx.order2.senderPublicKey)

              const amountAsset = tx.order1.assetPair.amountAsset
              const priceAsset = tx.order1.assetPair.priceAsset

              const amountDecimals = await getDecimals(amountAsset)
              const priceDecimals = await getDecimals(priceAsset)

              tx.pair = { amountAsset, amountDecimals, priceAsset, priceDecimals }

              delete tx.order1.assetPair
              delete tx.order2.assetPair

              break
            }
            case TransactionType.Transfer: {
              if (t.recipient.startsWith(aliasPrefix))
                t.recipient = await resolveAlias(t.recipient.substr(aliasPrefix.length))

              break
            }
            case TransactionType.MassTransfer: {
              await Promise.all(t.transfers
                .filter(tr => tr.recipient.startsWith(aliasPrefix))
                .map(async tr => await resolveAlias(tr.recipient.substr(aliasPrefix.length))))

              break
            }
          }
          return true
        } catch (error) {
          console.log(error)
        }
      }))
    ))

    return result
  }
  catch (ex) {
    console.log(`Retry from: ${from} to: ${to}`)
    return getBlocks(from, to)
  }
}

async function createDb() {
  const db = await MongoClient.connect(config.mongoUri)
  const blocksTable = await db.db('waves').createCollection('blocks')
  blocksTable.createIndex({ "transactions.id": 1 })
  blocksTable.createIndex({ _id: 1 })
}

function fetchBlocks(table: Collection<Block>, params: { batchSize: number, threads: number, from?: number, to?: number }): Promise<void> {
  return new Promise<void>(async (resolve, reject) => {

    let { batchSize, threads, from, to } = params

    if (!from)
      from = 1

    if (!to) {
      to = (await axios.get('https://nodes.wavesnodes.com/blocks/height')).data.height
      console.log(`Current height resolved to: ${to}`)
    }

    if (!to) {
      console.log(`Can't resolve current height`)
      return
    }

    if (from > to) {
      console.log(`From: (${from}) should be less than To: (${to})`)
      return
    }

    let blocksTotal = 0

    let c = to
    let cc = c
    let completed = false

    console.log(`Starting blocks fetch, from: ${from} to: ${to}`)
    bar.start(to - from, 0)
    const start = Date.now()

    async function fetch(from: number, to: number, forcedBatch?: number) {
      if (!forcedBatch && c < from)
        return

      const batchEnd = forcedBatch ? forcedBatch : c
      if (!forcedBatch)
        c -= batchSize
      const batchStart = forcedBatch ? forcedBatch : ((c < from) ? from : c)
      if (!forcedBatch)
        c -= 1

      const blocks = await getBlocks(batchStart, batchEnd)

      try {
        await Promise.all(blocks.map(block => {
          delete block._id
          return table.update(
            { "_id": block.height },
            { $set: block },
            { upsert: true }
          ).catch(e => console.log(e))
        }))

      }
      catch (ex) {
        console.log(ex)
        setTimeout(() => fetch(from, to, batchEnd), 1 + Math.random() * 100)
        return
      }

      cc -= blocks.length
      blocksTotal += blocks.length
      bar.update(blocksTotal)
      //console.log(`Blocks saved from: ${batchStart} to: ${batchEnd}`)
      //console.log(`Progress: ${blocksTotal}/${to - from}, Speed: ${Math.round(blocksTotal / ((Date.now() - start) / 1000))} blocks/s`)

      if (cc >= from && !completed)
        setTimeout(() => fetch(from, to), 1 + Math.random() * 100)
      else if (!completed) {
        completed = true
        bar.stop()
        console.log(`Blocks fetch complete, from: ${from} to: ${to} total: ${blocksTotal}`)
        resolve()
      }
    }

    for (let i = 0; i < threads; i++)
      fetch(from, to)
  })
}

async function main() {
  const db = await MongoClient.connect(config.mongoUri)
  const blocksTable = await db.db('waves').collection<Block>('blocks')
  //await fetchBlocks(blocksTable, { batchSize: 29, threads: 4, from: 1006900, to: 1008200 })
  await fetchBlocks(blocksTable, { batchSize: 29, threads: 4, from: 1015100, to: 1016400 })//to: 1012400 })
  await db.close()
}

main()










