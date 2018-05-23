import { MongoClient, Collection, Db, Mongos } from 'mongodb'
import { config } from './config';
import { Block, Transaction, TransactionType, Tx4, Tx11, WithSender, Tx7 } from './api/interfaces';
import { ServerRequest } from 'http';
import { airdrop, airdropAmounts } from './waves/send';

declare function emit(key: any, value: any): void

async function reduceToWavesDiff(out: string): Promise<void> {
  const db = (await MongoClient.connect(config.mongoUri))
  const waves = db.db('waves')
  const blocks = await waves.collection<Block>('blocks')

  await waves.collection('blocks').mapReduce(
    function (this: Block) {
      const block = this
      this.transactions.forEach(tx => {
        const t: any = tx
        t.height = block.height
        t._id = t.id
        delete t.id
        emit(t._id, t)
      })
    },
    function (key: number, tx: Transaction) {
      return { tx }
    },
    {
      // finalize: function (key: number, value: any) {
      //   return value.value
      // },
      out,
      //query: { _id: { $gt: 1006000 } }
    }
  )
  //  const log = await db.db('waves').collection('blocks_diff').find({})
  //  const r = (await log.toArray())
  //  console.log(r)
  //  console.log(r[0].value.values)
  //  console.log(Object.keys(r).length)
  //  console.log(from(r).distinct().toArray().length)
  await db.close()
}

reduceToWavesDiff('transactions')