import { MongoClient, Collection, Db, Mongos } from 'mongodb'
import { config } from './config';
import { Block, Transaction, TransactionType, Tx4, Tx11, WithSender, Tx7 } from './api/interfaces';
import { ServerRequest } from 'http';
import { airdrop, airdropAmounts } from './waves/send';

interface ValuesDiff {
  [index: string]: number
}
declare function emit(key: any, diff: any): void


export async function reduceToIncomingTransfers(table: Collection<Block>, out: string, from?: number) {
  const query = from ? { _id: { $gte: from } } : {}
  await table.mapReduce(
    function (this: Block) {

      const exhcangeWallets: any = {
        '3P4eeU7v1LMHQFwwT2GW9W99c6vZyytHajj': true,
        '3P31zvGdh6ai6JK6zZ18TjYzJsa1B83YPoj': true,
        '3P8wPvtfruNZjpqZACNjdqbtGRphwytdo6D': true,
        '3PMY8EZGetAjTZdrtFQ4rBZxD5bJEvUPDyX': true,
        '3P351tY1WibKh1f4EALHjoHfqWZnqs8gG9g': true,
        '3PGLgrBAt7PkFNVhPGTP1Wpc7dJfgZyy7S1': true,
        '3PAASSqnygiyYoQuqmXpwaSUJmRkqytwPaw': true,
        '3P7RHmhUEcRndM22SCySEp2NTC6yNv5APC1': true,
        '3PLrCnhKyX5iFbGDxbqqMvea5VAqxMcinPW': true,
        '3PAf7hzLsAK1WGEqtfcigNx6ktbvnD7T9BP': true,
        '3PHsnzgm3JLWdrPJDZXbSePZ2PPuDmzdb4Y': true,
        '3PE93oWhtuqJXhigh18ATTiUzxZau983vuD': true,
        '3PGXEsSmmiDwWXWVLrGFQjKFpGWC9NvUX9e': true, // https://liqui.io/
        '3PLPGmXoDNKeWxSgJRU5vDNogbPj7hJiWQx': true, // https://kuna.io/
      }

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
              if (exhcangeWallets[tx.sender])
                e(tx.recipient, tx.amount)
              else {
                e(tx.sender, -tx.amount)
              }
            }
            break;
          }
          case 11: {
            const tx = <Tx11>_tx
            if (!tx.assetId) {
              e(tx.sender, -tx.totalAmount)
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

            break;
          }
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