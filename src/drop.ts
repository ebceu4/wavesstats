import { MongoClient, Collection, Db, Mongos } from 'mongodb'
import { config } from './config';
import { Block, Transaction, TransactionType, Tx4, Tx11, WithSender, Tx7 } from './api/interfaces';
import { ServerRequest } from 'http';
import { airdrop, airdropAmounts } from './waves/send';
import { reduceToBalancesFromExchenges } from './reduceToBalancesFromExchenges';
import { reduceToWalletHeight } from './reduceToWalletHeight';
import { reduceToWavesBalance } from './reduceToWavesBalance';


async function computeWalletHeights(table: Collection, out: string) {
  await table.aggregate([
    //{ $replaceRoot: { newRoot: "$value" } },
    //{ $project: { v: { $objectToArray: "$value" } } },
    { $unwind: "$value.values" },
    { $group: { _id: "$value.values", height: { $min: "$_id" } } },
    { $out: out }
  ], { allowDiskUse: true }).toArray()
}

const wavesMinimalBalance = 1000000000
async function balanceByWallet(db: Db, table: string, from: number, to: number) {
  const blocks = await db.collection(table)

  const result = await blocks.aggregate([
    { $match: { $and: [{ _id: { $gte: from } }, { _id: { $lte: to } }] } },

    { $project: { v: { $objectToArray: "$value" } } },
    { $unwind: "$v" },
    { $replaceRoot: { newRoot: "$v" } },
    { $group: { _id: "$k", diff: { $sum: "$v" } } },
    {
      $lookup:
        {
          from: 'wallets',
          localField: '_id',
          foreignField: '_id',
          as: 'height',
        }
    },
    { $project: { diff: 1, height: { $arrayElemAt: ["$height", 0] } } },
    { $project: { diff: 1, height: "$height.height" } },
    { $match: { $and: [{ height: { $gte: from } }, { diff: { $gte: wavesMinimalBalance } }] } },
    { $out: "balance_change_for_period_" + from + "_" + to }

    // Hey you can even sort it without breaking things
    //{ $sort: { count: 1 } },
    // Output to a collection "output"
  ]).toArray()
  console.log(result)
}

enum Token {
  BCH = 'BCH',
  LIQUID = 'LIQUID',
  WCT = 'WCT',
}


async function walletsTotal(table: Collection, from: number, to: number): Promise<number> {
  return (await table.aggregate([
    { $match: { $and: [{ _id: { $gte: from } }, { _id: { $lte: to } }] } },

    { $project: { v: { $objectToArray: "$value" } } },
    { $unwind: "$v" },
    { $replaceRoot: { newRoot: "$v" } },
    { $group: { _id: "$k", diff: { $sum: "$v" } } },
    {
      $lookup:
        {
          from: 'wallets',
          localField: '_id',
          foreignField: '_id',
          as: 'height',
        }
    },
    { $project: { diff: 1, height: { $arrayElemAt: ["$height", 0] } } },
    { $project: { diff: 1, height: "$height.height" } },
    { $match: { $and: [{ height: { $gte: from } }, { diff: { $gte: wavesMinimalBalance } }] } },
    { $group: { _id: null, total: { $sum: "$diff" } } }
  ]).toArray())[0].total
}

interface TokenInfo {
  id: string,
  symbol: string,
  decimals: number,
  priceInUSD: number
}

async function prepareDrop(table: Collection, options: { from: number, to: number, tokens: TokenInfo[], totalUSD: number }) {
  const { from, to, tokens, totalUSD } = options
  const perTokenUSD = totalUSD / tokens.length
  const addFields = tokens
    .map(t => ({ amountToSpend: perTokenUSD / t.priceInUSD * Math.pow(10, t.decimals), token: t }))
    .reduce((acc: any, t) => { console.log(`${t.token.symbol}: ${Math.round(t.amountToSpend / Math.pow(10, t.token.decimals))}`); acc[t.token.symbol] = { $trunc: { $multiply: ["$share", t.amountToSpend] } }; return acc }, {})

  const aggregateAndFilterByHeight: any[] = [
    { $match: { $and: [{ _id: { $gte: from } }, { _id: { $lte: to } }] } },
    { $project: { v: { $objectToArray: "$value" } } },
    { $unwind: "$v" },
    { $replaceRoot: { newRoot: "$v" } },
    { $group: { _id: "$k", diff: { $sum: "$v" } } },
    {
      $lookup:
        {
          from: 'wallets',
          localField: '_id',
          foreignField: '_id',
          as: 'height',
        }
    },
    { $project: { diff: 1, height: { $arrayElemAt: ["$height", 0] } } },
    { $project: { diff: 1, height: "$height.height" } },
    { $match: { $and: [{ height: { $gte: from } }, { diff: { $gte: wavesMinimalBalance } }] } },
  ]

  const total = (await table.aggregate(aggregateAndFilterByHeight.concat([{ $group: { _id: null, total: { $sum: "$diff" } } }])).toArray())[0].total

  const out = "drop_" + from + "_" + to

  const result = await table.aggregate(aggregateAndFilterByHeight.concat([
    {
      $addFields: {
        share:
          { $divide: ["$diff", total] }
      }
    },
    { $addFields: addFields },
    { $out: out }
  ])
  ).toArray()
  return out
}

const conditions: { [i: string]: any } = {
  BCH: { BCH_DONE: null },
  LIQUID: { LIQUID_DONE: null },
  WCT: { WCT_DONE: null },
}

async function getWallets(db: Db, table: string, limit: number, token: TokenInfo) {
  const items = await db.collection(table)
  const find: { [i: string]: any } = {}
  find[token.symbol + '_DONE'] = null
  find[token.symbol] = { $gt: 0 }
  return await items.find(find, { limit }).toArray()
}

async function output() {
  const db = await MongoClient.connect(config.mongoUri)
  db.db('waves').collection('drop_1002585_1004100').aggregate([
    { $project: { _id: 1, balance: { $divide: ["$diff", 100000000] }, share: 1, BCH: { $divide: ["$BCH", 100000000] }, LIQUID: { $divide: ["$LIQUID", 100000000] }, WCT: { $divide: ["$WCT", 100] } } },
    { $out: 'drop_tmp' }
  ]).toArray()
}

async function markWalletsDropped(db: Db, table: string, wallets: string[], token: TokenInfo) {
  const items = await db.collection(table)
  const c: { [i: string]: boolean } = {}
  c[token.symbol + '_DONE'] = true

  await Promise.all(wallets.map(w =>
    items.update(
      { "_id": w },
      { $set: c },
      { upsert: false }
    )))
}

async function drop(db: Db, table: string, token: TokenInfo) {
  const tokenSymbol = token.symbol
  console.log(`${tokenSymbol} drop`)

  let total = 0
  let totalAmount = 0
  let r = []
  do {
    r = await getWallets(db, table, 100, token)
    try {
      if (r.length > 0) {
        const d = await airdropAmounts(token.id, r.map(x => ({ address: x._id, amount: x[tokenSymbol] })), config.seed)
        if (d.status == 200)
          await markWalletsDropped(db, table, r.map(x => x._id), token)

        total += r.length
        console.log(`Progress: ${total}`)
      }
    }
    catch (ex) {
      console.log(ex)
    }
  } while (r.length > 0)

  console.log(`${tokenSymbol} completed`)
}

async function makeDrop(db: Db, table: string, tokens: TokenInfo[]) {
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    await drop(db, table, token)
  }
}

async function main() {
  const db = (await MongoClient.connect(config.mongoUri))
  const waves = db.db('waves')
  const blocks = await waves.collection<Block>('blocks')

  async function status() {
    const currentOp = await waves.executeDbAdminCommand({
      currentOp: true,
    })
    const r = currentOp.inprog.filter((o: any) => o.msg != null).forEach((o: any) => console.log(o.msg))
  }

  const tokens: TokenInfo[] = [
    { id: 'zMFqXuoyrn5w17PFurTqxB7GsS71fp9dfk6XFwxbPCy', symbol: 'BCH', priceInUSD: 1035, decimals: 8 },
    { id: '7FzrHF1pueRFrPEupz6oiVGTUZqe8epvC7ggWUx8n1bd', symbol: 'LIQUID', priceInUSD: 2.12, decimals: 8 },
    { id: 'DHgwrRvVyqJsepd32YbBqUeDH4GJ1N984X8QoekjgH8J', symbol: 'WCT', priceInUSD: 0.59, decimals: 2 },
  ]

  const from = 1002585
  const to1 = 1027000
  //const to2 = 1025000
  //const to2 = 1020400

  const interval = setInterval(status, 1000)
  await reduceToBalancesFromExchenges(blocks, "blocks_exchanges", from)
  await reduceToWavesBalance(blocks, "blocks_diff", from)
  await reduceToWalletHeight(blocks, "blocks_addr", from)
  const blocks_addr = await waves.collection('blocks_addr')
  const blocks_diff = await waves.collection('blocks_diff')
  const blocks_exchanges = await waves.collection('blocks_exchanges')
  // await computeWalletHeights(blocks_addr, "wallets")
  const dropTable = await prepareDrop(blocks_exchanges, { tokens, totalUSD: 5000, from, to: to1 })
  // const dropTable2 = await prepareDrop(blocks_exchanges, { tokens, totalUSD: 5000, from, to: to2 })

  await makeDrop(waves, 'drop_1002585_' + to1, tokens)
  //await makeDrop(waves, 'drop_1002585_' + to2, tokens)
  await db.close()
  clearInterval(interval)
}

main()