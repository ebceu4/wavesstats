import { MongoClient } from 'mongodb'
import { config } from './config';
import { from } from 'linq';
import { Block, Transaction, TransactionType, Tx4, Tx11, WithSender, Tx7 } from './api/interfaces';
import { TESTNET_CONFIG } from '@waves/waves-api';
import BigNumber from 'bignumber.js';
import { Byte } from '@waves/waves-api/raw/src/classes/ByteProcessor';
import { ServerRequest } from 'http';


declare function emit(key: any, diff: ValuesDiff): void
declare function NumberLong(n: number | string): number

interface ValuesDiff {
  [index: string]: number
}

interface IDiff {
  waves: ValuesDiff
}

async function main() {
  const db = await MongoClient.connect(config.mongoUri)
  const blocks = await db.db('waves').createCollection('blocks')




  const result = await blocks.mapReduce(
    function (this: Block) {

      const toDiff = (block: Block, _tx: Transaction) => {
        const e = (adress: string, value: number) => {
          const diff: ValuesDiff = {}
          diff[adress] = value
          emit(block._id, diff)
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
    }, {
      finalize:
        function (key: number, value: number) {
          return value
        },
      out: { replace: "result" }
    }
  )

  const log = await db.db('waves').collection('result').find({})

  const r = (await log.toArray())

  console.log(r)
  //console.log(r[0].value.values)

  //  console.log(Object.keys(r).length)
  //  console.log(from(r).distinct().toArray().length)

  await db.close()
}

main()