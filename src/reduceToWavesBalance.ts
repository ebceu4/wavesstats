import { MongoClient, Collection, Db, Mongos } from 'mongodb'
import { config } from './config';
import { Block, Transaction, TransactionType, Tx4, Tx11, WithSender, Tx7 } from './api/interfaces';
import { ServerRequest } from 'http';
import { airdrop, airdropAmounts } from './waves/send';
import { reduceToBalancesFromExchenges } from './reduceToBalancesFromExchenges';

interface ValuesDiff {
  [index: string]: number
}

declare function emit(key: any, diff: any): void
declare function NumberLong(n: number | string): number

export async function reduceToWavesBalance(table: Collection<Block>, out: string, from?: number): Promise<void> {
  const query = from ? { _id: { $gte: from } } : {}
  await table.mapReduce(
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
            if (tx.assetId == null) {
              e(tx.sender, -tx.amount)
              e(tx.recipient, tx.amount)
            }

            if (tx.feeAsset == null) {
              e(tx.sender, -tx.fee)
              e(block.generator, tx.fee)
            }

            break;
          }
          case 11: {
            const tx = <Tx11>_tx
            if (tx.assetId == null) {
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
            if (tx.pair.priceAsset == null) {
              const value = Math.round((tx.price * tx.amount) / Math.pow(10, 8 + tx.pair.priceDecimals - tx.pair.amountDecimals))
              e(buyer, -value - tx.buyMatcherFee)
              e(seller, +value - tx.sellMatcherFee)
            }
            else if (tx.pair.amountAsset == null) {
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
      this.transactions.forEach(t => toDiff(this, t))
    },
    function (key: number, values: ValuesDiff[]) {
      return values.reduce((a, b) => Object.keys(b).reduce((acc, key) => { acc[key] = (a[key] ? a[key] + b[key] : b[key]); return acc }, a), {})
    },
    {
      query,
      // finalize: function (key: number, value: number) {
      //   return value
      // },
      out: { merge: out },
    }
  )
  //  const log = await db.db('waves').collection('blocks_diff').find({})
  //  const r = (await log.toArray())
  //  console.log(r)
  //console.log(r[0].value.values)
  //  console.log(Object.keys(r).length)
  //  console.log(from(r).distinct().toArray().length)
}
