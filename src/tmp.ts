import { MongoClient, Collection, Db, Mongos } from 'mongodb'
import { config } from './config';
import { Block, Transaction, TransactionType, Tx4, Tx11, WithSender, Tx7 } from './api/interfaces';
import { ServerRequest } from 'http';
import { airdrop, airdropAmounts } from './waves/send';
import { readFileSync } from 'fs';

interface ValuesDiff {
  [index: string]: number
}
declare function emit(key: any, diff: any): void

async function addWallets(db: Db, table: string, wallets: { [address: string]: number }) {
  const items = await db.collection(table)


  const w = Object.keys(wallets).map((k: string) => ({ _id: k, MRT: wallets[k] })).filter(x => x.MRT > 0)

  return items.insertMany(w)


  // Object.keys(wallets).map((w: any) => {
  //   return items.update(
  //     { "_id": w },
  //     { $set: { MRT: wallets[w] } },
  //     { upsert: false }
  //   )
  // })


  //return Promise.all()
}

async function main() {
  const db = (await MongoClient.connect(config.mongoUri))
  const waves = db.db('waves')

  const wallets = JSON.parse(readFileSync('MRT_distribution', { encoding: 'utf8' }))
  await addWallets(waves, "wallets_MRT", wallets)
  await db.close()
  //clearInterval(interval)
}

main()