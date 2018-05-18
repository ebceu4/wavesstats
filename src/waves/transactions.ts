export type IDataEntryType = 'integer' | 'boolean' | 'binary'

export interface IDataEntry {
  key: string,
  type: IDataEntryType,
  value: number | boolean | string
}

export interface IDataTransaction {
  version: number,
  senderPublicKey: string,
  data: IDataEntry[],
  fee: number,
  timestamp: number
}

export interface ISignedDataTransacton {
  type: number,
  version: number,
  senderPublicKey: string,
  data: IDataEntry[],
  fee: number,
  timestamp: number,
  proofs: string[]
}

export interface ITransfer {
  recipient: string
  amount: number
}

export interface IMassTransferTransaction {
  fee: number
  timestamp: number
  assetId?: string
  attachment?: string
  transfers: ITransfer[]
}

export interface ISignedMassTransferTransaction extends IMassTransferTransaction {
  type: number
  version: number
  senderPublicKey: string
  proofs: string[]
}