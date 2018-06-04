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

export async function reduceToWalletHeight
(table: Collection<Block>, out: string, from?: number) {
  const query = from ? { _id: { $gte: from } } : {}
  await table.mapReduce(
    function (this: Block) {

      const toDiff = (block: Block, _tx: Transaction) => {
        const e = (address: string, type: string, timestamp?: number) => {
          emit(block.height, address)
        }

        let tx
        switch (_tx.type) {
          case 4: {
            const tx = <Tx4>_tx
            e(tx.sender, 'sender')
            e(tx.recipient, 'recipient')
            break;
          }
          case 11: {
            const tx = <Tx11>_tx
            tx.transfers.forEach(t => {
              e(t.recipient, 'recipient')
            })
            e(tx.sender, 'sender')
            break;
          }
          case 7: {
            const tx = <Tx7>_tx
            const buyer = tx.order1.orderType == 'buy' ? tx.order1.sender : tx.order2.sender
            const seller = tx.order1.orderType == 'sell' ? tx.order1.sender : tx.order2.sender
            const matcher = tx.sender
            e(buyer, 'buyer')
            e(seller, 'seller')
            e(matcher, 'matcher')
            break;
          }
        }

        {
          const tx = <WithSender>_tx
          e(tx.sender, 'sender')
          e(block.generator, 'generator', block.timestamp)
        }
      }
      this.transactions.forEach(t => toDiff(this, t))
    },
    function (key: number, values: ValuesDiff[]) {
      return { values }
    },
    {
      query,
      // finalize: function (key: number, value: number) {
      //   return value
      // },
      out: { merge: out }
    }
  )

  //const log = await db.db('waves').collection('blocks_diff').find({})
  //const r = (await log.toArray())
  //console.log(r)
  //console.log(r[0].value.values)
  //  console.log(Object.keys(r).length)
  //  console.log(from(r).distinct().toArray().length)
}
