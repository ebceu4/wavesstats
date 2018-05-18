import axios from 'axios'
import { IMassTransferTransaction, IDataTransaction } from './transactions'
import { signMassTranserTransaction, signDataTransaction } from './sign'

const apiBase = 'http://nodes.wavesnodes.com/'

export const sendMassTransferTransaction = async (tx: IMassTransferTransaction, seed: string) => {
  const signedTx = signMassTranserTransaction(tx, seed)
  return axios.post(apiBase + 'transactions/broadcast', signedTx)
}

export const sendDataTransaction = async (tx: IDataTransaction, seed: string) => {
  const signedTx = signDataTransaction(tx, seed)
  return axios.post(apiBase + 'transactions/broadcast', signedTx)
}

export const airdrop = async (assetId: string, amount: number, addresses: string[], seed: string) => {
  const signedTx = signMassTranserTransaction({
    assetId,
    fee: 100000 + 50000 * addresses.length,
    transfers: addresses.map(a => ({ recipient: a, amount })),
    timestamp: Date.now(),
  }, seed)
  return axios.post(apiBase + 'transactions/broadcast', signedTx)
}

export const airdropAmounts = async (assetId: string, addressesAndAmounts: { address: string, amount: number }[], seed: string) => {
  const signedTx = signMassTranserTransaction({
    assetId,
    fee: 100000 + 50000 * addressesAndAmounts.length,
    transfers: addressesAndAmounts.map(a => ({ recipient: a.address, amount: a.amount })),
    timestamp: Date.now(),
  }, seed)
  return axios.post(apiBase + 'transactions/broadcast', signedTx)
}
