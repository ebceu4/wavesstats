import { MongoClient } from 'mongodb'
import { config } from './config';
import { from } from 'linq';
import { Block, Transaction, TransactionType, Tx4, Tx11, WithSender, Tx7 } from './api/interfaces';
import { TESTNET_CONFIG } from '@waves/waves-api';
import BigNumber from 'bignumber.js';
import { Byte } from '@waves/waves-api/raw/src/classes/ByteProcessor';
import { ServerRequest } from 'http';
import { airdrop, airdropAmounts } from './waves/send';


declare function emit(key: any, diff: ValuesDiff): void
declare function emit(key: any, diff: any): void
declare function NumberLong(n: number | string): number

interface ValuesDiff {
  [index: string]: number
}

interface IDiff {
  waves: ValuesDiff
}

async function reduceToWavesDiff(out: string) {
  const db = await MongoClient.connect(config.mongoUri)
  const blocks = await db.db('waves').createCollection('blocks')

  const result = await blocks.mapReduce(
    function (this: Block) {

      const toDiff = (block: Block, _tx: Transaction) => {
        const e = (adress: string, value: number) => {
          const diff: ValuesDiff = {}
          diff[adress] = value
          emit(block.height, diff)
        }
        let tx
        switch (_tx.type) {
          case 4: {
            const tx = <Tx4>_tx
            if (!tx.assetId) {
              e(tx.sender, -tx.amount)
              e(tx.recipient, tx.amount)
            }

            if (!tx.feeAsset) {
              e(tx.sender, -tx.fee)
              e(block.generator, tx.fee)
            }

            break;
          }
          case 11: {
            const tx = <Tx11>_tx
            if (!tx.assetId) {
              tx.transfers.forEach(t => {
                e(t.recipient, t.amount)
                e(tx.sender, -t.amount)
              })
            }

            break;
          }
          case 7: {
            const tx = <Tx7>_tx
            const buyer = tx.order1.orderType == 'buy' ? tx.order1.sender : tx.order2.sender
            const seller = tx.order1.orderType == 'sell' ? tx.order1.sender : tx.order2.sender
            const matcher = tx.sender
            if (tx.order1.assetPair.priceAsset == null) {
              const value = Math.round((tx.price * tx.amount) / Math.pow(10, 8 + tx.pair.priceDecimals - tx.pair.amountDecimals))
              e(buyer, -value - tx.buyMatcherFee)
              e(seller, +value - tx.sellMatcherFee)
            }
            else if (tx.order1.assetPair.amountAsset == null) {
              e(buyer, + tx.amount - tx.buyMatcherFee)
              e(seller, -tx.amount - tx.sellMatcherFee)
            }

            e(matcher, tx.buyMatcherFee + tx.sellMatcherFee)
            break;
          }
        }

        if (_tx.type != 4 && (<WithSender>_tx).sender) {
          const tx = <WithSender>_tx
          e(tx.sender, -_tx.fee)
          e(block.generator, _tx.fee)
        }
      }
      //3P8tEm97ifGFK6fyLm6yxWabFsbDqyUugnb
      //if (this._id == '51krrYm3TTezVtNLqri3KMSFA8JM6xry5koQTaBHTxjePdgC2NV7Lkf2YwG1GfiEkMLv524reRXECmUicY8VmgSa')
      this.transactions.forEach(t => toDiff(this, t))
    },
    function (key: number, values: ValuesDiff[]) {
      //return { values: values.filter(v => v['3P8tEm97ifGFK6fyLm6yxWabFsbDqyUugnb']) }
      return values.reduce((a, b) => Object.keys(b).reduce((acc, key) => { acc[key] = (a[key] ? a[key] + b[key] : b[key]); return acc }, a), {})
    },
    {
      // finalize: function (key: number, value: number) {
      //   return value
      // },
      out
    }
  )

  const log = await db.db('waves').collection('blocks_diff').find({})

  const r = (await log.toArray())

  console.log(r)
  //console.log(r[0].value.values)
  //  console.log(Object.keys(r).length)
  //  console.log(from(r).distinct().toArray().length)

  await db.close()
}

async function reduceToAddresses(out: string) {

  const db = await MongoClient.connect(config.mongoUri)
  const blocks = await db.db('waves').createCollection('blocks')

  const result = await blocks.mapReduce(
    function (this: Block) {

      const toDiff = (block: Block, _tx: Transaction) => {
        const e = (adress: string) => {
          emit(block.height, adress)
        }
        let tx
        switch (_tx.type) {
          case 4: {
            const tx = <Tx4>_tx
            e(tx.sender)
            e(tx.recipient)
            break;
          }
          case 11: {
            const tx = <Tx11>_tx
            tx.transfers.forEach(t => {
              e(t.recipient)
            })
            e(tx.sender)
            break;
          }
          case 7: {
            const tx = <Tx7>_tx
            const buyer = tx.order1.orderType == 'buy' ? tx.order1.sender : tx.order2.sender
            const seller = tx.order1.orderType == 'sell' ? tx.order1.sender : tx.order2.sender
            const matcher = tx.sender
            e(buyer)
            e(seller)
            e(matcher)
            break;
          }
        }

        {
          const tx = <WithSender>_tx
          e(tx.sender)
          e(block.generator)
        }
      }
      //3P8tEm97ifGFK6fyLm6yxWabFsbDqyUugnb
      //if (this._id == '51krrYm3TTezVtNLqri3KMSFA8JM6xry5koQTaBHTxjePdgC2NV7Lkf2YwG1GfiEkMLv524reRXECmUicY8VmgSa')
      this.transactions.forEach(t => toDiff(this, t))
    },
    function (key: number, values: ValuesDiff[]) {
      return { values }
    },
    {
      // finalize: function (key: number, value: number) {
      //   return value
      // },
      out
    }
  )

  const log = await db.db('waves').collection('blocks_diff').find({})

  const r = (await log.toArray())

  console.log(r)
  //console.log(r[0].value.values)
  //  console.log(Object.keys(r).length)
  //  console.log(from(r).distinct().toArray().length)

  await db.close()

}

async function computeWalletHeights(sourceTable: string, out: string) {
  const db = await MongoClient.connect(config.mongoUri)
  const blocks = await db.db('waves').collection(sourceTable)

  const result = await blocks.aggregate([
    //{ $replaceRoot: { newRoot: "$value" } },
    //{ $project: { v: { $objectToArray: "$value" } } },
    { $unwind: "$value.values" },
    { $group: { _id: "$value.values", height: { $min: "$_id" } } },
    { $out: out }
  ], { allowDiskUse: true }).toArray()
  console.log(result)
  await db.close()
}

const wavesMinimalBalance = 1000000000
async function balanceByWallet(from: number, to: number) {
  const db = await MongoClient.connect(config.mongoUri)
  const blocks = await db.db('waves').collection('blocks_diff')

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
  await db.close()
}

const tokens: { [i: string]: { id: string, price: number, decimals: number } } = {
  BCH: { id: 'zMFqXuoyrn5w17PFurTqxB7GsS71fp9dfk6XFwxbPCy', price: 1241, decimals: 8 },
  LIQUID: { id: '7FzrHF1pueRFrPEupz6oiVGTUZqe8epvC7ggWUx8n1bd', price: 3.36, decimals: 8 },
  WCT: { id: 'DHgwrRvVyqJsepd32YbBqUeDH4GJ1N984X8QoekjgH8J', price: 0.82, decimals: 2 },
}

const total = 170600603611183
const totalUSD = 5000
const usdPerToken = totalUSD / 3

const tokensToSpend = {
  BCH: usdPerToken / tokens.BCH.price * Math.pow(10, tokens.BCH.decimals),
  LIQUID: usdPerToken / tokens.LIQUID.price * Math.pow(10, tokens.LIQUID.decimals),
  WCT: usdPerToken / tokens.WCT.price * Math.pow(10, tokens.WCT.decimals),
}

async function wallets(from: number, to: number) {
  const db = await MongoClient.connect(config.mongoUri)
  const blocks = await db.db('waves').collection('blocks_diff')

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
    {
      $addFields: {
        share:
          { $divide: ["$diff", total] }
      }
    },
    {
      $addFields: {
        BCH:
          { $trunc: { $multiply: ["$share", tokensToSpend.BCH] } },
        LIQUID:
          { $trunc: { $multiply: ["$share", tokensToSpend.LIQUID] } },
        WCT:
          { $trunc: { $multiply: ["$share", tokensToSpend.WCT] } }
      }
    },
    { $out: "drop_" + from + "_" + to }


    // Hey you can even sort it without breaking things
    //{ $sort: { count: 1 } },
    // Output to a collection "output"
  ]).toArray()
  await db.close()

  return result
}

const conditions: { [i: string]: any } = {
  BCH: { BCH_DONE: null },
  LIQUID: { LIQUID_DONE: null },
  WCT: { WCT_DONE: null },
}

async function getWallets(table: string, limit: number, condition: any) {
  const db = await MongoClient.connect(config.mongoUri)
  const items = await db.db('waves').collection(table)

  const r = await items.find(condition, { limit }).toArray()

  await db.close()
  return r
}

async function ddd() {
  const db = await MongoClient.connect(config.mongoUri)
  db.db('waves').collection('drop_1002585_1004100').aggregate([
    { $project: { _id: 1, balance: { $divide: ["$diff", 100000000] }, share: 1, BCH: { $divide: ["$BCH", 100000000] }, LIQUID: { $divide: ["$LIQUID", 100000000] }, WCT: { $divide: ["$WCT", 100] } } },
    { $out: 'drop_tmp' }
  ]).toArray()
}



async function setWallets(table: string, wallets: string[], condition: any) {
  const db = await MongoClient.connect(config.mongoUri)
  const items = await db.db('waves').collection(table)

  const c: { [i: string]: boolean } = {}
  Object.assign(c, condition)
  Object.keys(c).forEach(k => c[k] = true)

  const r = await Promise.all(wallets.map(w =>
    items.update(
      { "_id": w },
      { $set: c },
      { upsert: false }
    )))

  await db.close()
  return r
}



const countPart = (wavesBalance: number) => ((wavesBalance / total) * usdPerToken)

async function drop(token: string) {
  //const rr = await wallets(1002585, 1004100)
  //console.log(tokensToSpend)
  console.log(`${token} drop`)

  let total = 0
  let totalAmount = 0
  let r = []
  do {
    r = await getWallets('drop_1002585_1004100', 100, conditions[token])
    try {
      if (r.length > 0)
        await airdropAmounts(tokens[token].id, r.map(x => {
          totalAmount += parseInt(x[token]);
          let a = x._id
          if (a == 'alias:W:soldavos')
            a = '3PML8KeFaBeWrE5akh14VZ9HBnFyZvJGHfH'
          if (a == 'alias:W:tharde')
            a = '3PK5KEfW8VRqieT3dHDrjmHtQkjeX8uPp2x'
          if (a == 'alias:W:waveshodl')
            a = '3PJdZgQ9rvtQQyQMqGU3hdgNrnZKNfrBya4'
          return {
            address: a, amount: parseInt(x[token])
          }
        }), config.seed)
      await setWallets('drop_1002585_1004100', r.map(x => x._id), conditions[token])
      total += r.length
      console.log(`Progress: ${total}, amount: ${totalAmount}`)
    }
    catch (ex) {
      console.log(ex)
    }

  } while (r.length > 0)

  console.log(`${token} completed`)
}

async function d() {
  //await drop('BCH')
  //await drop('LIQUID')
  //await drop('WCT')
}

//1.241 BCH
//3.36 LIQUID
//0.82 WCT

//reduceToWavesDiff("blocks_diff")
//reduceToAddresses("blocks_addr")
//balanceByWallet(1002585, 1004100)
//computeWalletHeights("blocks_addr", "wallets")

ddd()