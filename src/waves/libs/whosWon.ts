import axios from 'axios'
import * as _ from 'linq'


interface ITransferTransaction {
  type: number,
  id: string,
  sender: string,
  senderPublicKey: string,
  fee: number,
  timestamp: number,
  signature: string,
  recipient: string,
  assetId: string,
  amount: number,
  feeAsset: string,
  attachment: string,
  height: number
}

const assets = [
  '7FV44UeijbdjbQpDhqt35TcuJ3kb9USFx25BTaUWop2X',	//ALAN	
  '4fEMX9677YjBntx1XEEyeYFVS25xBoGqQvu6Juf6UuMM',	//ZIGM	
  '8kPjEfx2mdrdRGKc7V7ercPdURNaJZMw1TDaMgsi98fR',	//JOHN	
  'FNFEwvwXEW2w8bzCFhMFL51xXkCt8xAB8gzxqmYRFVx9',	//VLAD	
  '6H97QsMJWCTRTzZ3wGKofKUbndxbQAaYMsMC2JFjXBy9',	//YURI	
]

const names = {
  '7FV44UeijbdjbQpDhqt35TcuJ3kb9USFx25BTaUWop2X': 'ALAN',
  '4fEMX9677YjBntx1XEEyeYFVS25xBoGqQvu6Juf6UuMM': 'ZIGM',
  '8kPjEfx2mdrdRGKc7V7ercPdURNaJZMw1TDaMgsi98fR': 'JOHN',
  'FNFEwvwXEW2w8bzCFhMFL51xXkCt8xAB8gzxqmYRFVx9': 'VLAD',
  '6H97QsMJWCTRTzZ3wGKofKUbndxbQAaYMsMC2JFjXBy9': 'YURI'
}

axios.get('https://nodes.wavesnodes.com/transactions/address/3PG3JmVh1czUhvg8stVwFY8zXkqVJBqeeJw/limit/10000').then(x => x.data[0])
  .then((x: any[]) => _.from(x).where(x => x.type == 4).select((tx: ITransferTransaction) => tx)
    .where(tx => assets.includes(tx.assetId))
    .groupBy(tx => tx.sender).where(x => x.count() >= 5)
    .select(x => {
      const transactions = x.groupBy(x => x.assetId).select(x => {
        const timestamp = x.select(y => y.timestamp).orderBy(x => x).first()
        return { asset: names[x.key()], timestamp, time: new Date(timestamp).toLocaleString() }
      }).toArray()
      const lastTransaction = _.from(transactions).orderBy(x => x.timestamp).last()
      const address = x.key()
      return { address, lastTransaction, transactions }
    }).orderByDescending(x => x.lastTransaction.timestamp).toArray().forEach(x => console.log(`${x.address}   ${x.lastTransaction.time}`)))

