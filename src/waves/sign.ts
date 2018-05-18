import * as Long from 'long'
import * as Crypto from './crypto'
import { IMassTransferTransaction, ITransfer, ISignedMassTransferTransaction, IDataTransaction, ISignedDataTransacton, IDataEntry } from './transactions'
import { Utils } from 'linq';
import * as base58 from './libs/base58';
import { write, IWriteBuffer } from './buffer';

export function signDataTransaction(tx: IDataTransaction, seed: string): ISignedDataTransacton {

  const keyPair = Crypto.buildKeyPairBytes(seed)

  function writeEntry(buffer: IWriteBuffer, entry: IDataEntry) {
    const keyBytes = new Buffer(entry.key)
    buffer.writeShortUnsigned(keyBytes.length)
    buffer.writeBytes(keyBytes)

    switch (entry.type) {
      case 'integer':
        buffer.writeByte(0)
        buffer.writeLong(Long.fromNumber(<number>entry.value))
        break
      case 'boolean':
        buffer.writeByte(1)
        buffer.writeByte(entry.value === true ? 1 : 0)
        break
      case 'binary':
        const bytes = base58.decode(<string>entry.value)
        buffer.writeByte(2)
        buffer.writeShortUnsigned(bytes.length)
        buffer.writeBytes(bytes)
        break
    }
  }

  const buffer = write()
  buffer.writeByte(12)
  buffer.writeByte(tx.version)
  const senderPubKeyBytes = base58.decode(tx.senderPublicKey)
  buffer.writeBytes(senderPubKeyBytes)
  buffer.writeShortUnsigned(tx.data.length)
  tx.data.forEach(entry => writeEntry(buffer, entry))
  buffer.writeLong(Long.fromNumber(tx.timestamp))
  buffer.writeLong(Long.fromNumber(tx.fee))
  const proof = Crypto.signBytes(buffer.raw(), keyPair.privateKey)

  return {
    version: tx.version,
    data: tx.data,
    senderPublicKey: tx.senderPublicKey,
    proofs: [proof],
    timestamp: tx.timestamp,
    fee: tx.fee,
    type: 12
  }
}

export function signMassTranserTransaction(tx: IMassTransferTransaction, seed: string): ISignedMassTransferTransaction {
  const keyPair = Crypto.buildKeyPairBytes(seed)
  const senderPublicKey = ''
  const assetIdBytes = tx.assetId ? Crypto.concatUint8Arrays(Uint8Array.from([1]), base58.decode(tx.assetId)) : Uint8Array.from([0])

  const buffer = write()
    .writeByte(11)
    .writeByte(1)
    .writeBytes(keyPair.publicKey)
    .writeBytes(assetIdBytes)
    .writeShortUnsigned(tx.transfers.length)
  tx.transfers.forEach(t => {
    buffer.writeBytes(base58.decode(t.recipient))
      .writeLong(Long.fromNumber(t.amount))
  })
  buffer.writeLong(Long.fromNumber(tx.timestamp))
    .writeLong(Long.fromNumber(tx.fee))
  if (tx.attachment) {
    const bytes = base58.decode(tx.attachment)
    buffer.writeShortUnsigned(bytes.length)
      .writeBytes(bytes)
  } else {
    buffer.writeShortUnsigned(0)
  }

  const proof = Crypto.signBytes(buffer.raw(), keyPair.privateKey)

  return {
    type: 11,
    version: 1,
    fee: tx.fee,
    transfers: tx.transfers,
    assetId: tx.assetId,
    attachment: tx.attachment,
    timestamp: tx.timestamp,
    senderPublicKey: base58.encode(keyPair.publicKey),
    proofs: [proof]
  }

}