import * as CryptoJS from 'crypto-js';
import axlsign from './libs/axlsign';
import { encode, decode } from './libs/base58';
import * as blake from './libs/blake2b';
import converters from './libs/converters';
import secureRandom from './libs/secure-random';
import { keccak256 } from './libs/sha3';

const networkByte = 'H'.charCodeAt(0)

export interface IKeyPair {
  readonly privateKey: string;
  readonly publicKey: string;
}

export interface IKeyPairBytes {
  readonly privateKey: Uint8Array;
  readonly publicKey: Uint8Array;
}

export function concatUint8Arrays(...args: Uint8Array[]): Uint8Array {
  if (args.length < 2)
    throw new Error('Two or more Uint8Array are expected');

  if (!(args.every((arg) => arg instanceof Uint8Array)))
    throw new Error('One of arguments is not a Uint8Array');

  const count = args.length;
  const sumLength = args.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Uint8Array(sumLength);

  let curLength = 0;

  for (let i = 0; i < count; i++) {
    result.set(args[i], curLength);
    curLength += args[i].length;
  }

  return result;
}

function sha256(input: Array<number> | Uint8Array | string): Uint8Array {

  let bytes;
  if (typeof input === 'string') {
    bytes = converters.stringToByteArray(input);
  } else {
    bytes = input;
  }

  const wordArray = converters.byteArrayToWordArrayEx(Uint8Array.from(bytes));
  const resultWordArray = CryptoJS.SHA256(wordArray);

  return converters.wordArrayToByteArrayEx(resultWordArray);

}

function blake2b(input) {
  return blake.blake2b(input, null, 32);
}

function keccak(input) {
  return (keccak256 as any).array(input);
}

function hashChain(input: Uint8Array): Array<number> {
  return keccak(blake2b(input));
}

function buildSeedHash(seedBytes: Uint8Array): Uint8Array {
  const nonce = new Uint8Array(converters.int32ToBytes(0, true));
  const seedBytesWithNonce = concatUint8Arrays(nonce, seedBytes);
  const seedHash = hashChain(seedBytesWithNonce);
  return sha256(seedHash);
}

function strengthenPassword(password: string, rounds: number = 5000): string {
  while (rounds--) password = converters.byteArrayToHexString(sha256(password));
  return password;
}

export function buildKeyPairBytes(seed: string): IKeyPairBytes {
  if (!seed || typeof seed !== 'string')
    throw new Error('Missing or invalid seed phrase');

  const seedBytes = Uint8Array.from(converters.stringToByteArray(seed));
  const seedHash = buildSeedHash(seedBytes);
  const keys = axlsign.generateKeyPair(seedHash);

  return {
    privateKey: keys.private,
    publicKey: keys.public
  };
}

export function signBytes(bytes: Uint8Array, privateKey: string | Uint8Array): string {

  if (!bytes || !(bytes instanceof Uint8Array))
    throw new Error('Missing or invalid data');

  if (!privateKey)
    throw new Error('Missing private key');

  const privateKeyBytes = (typeof privateKey !== 'string') ? privateKey : decode(privateKey)

  if (privateKeyBytes.length !== 32)
    throw new Error('Invalid public key');

  const signature = axlsign.sign(privateKeyBytes, bytes, secureRandom.randomUint8Array(64));
  return encode(signature);
}

export function buildTransactionId(dataBytes: Uint8Array): string {

  if (!dataBytes || !(dataBytes instanceof Uint8Array)) {
    throw new Error('Missing or invalid data');
  }

  const hash = blake2b(dataBytes);
  return encode(hash);

}

export function createKeyPair(seed: string): IKeyPair {
  const r = buildKeyPairBytes(seed)
  return { privateKey: encode(r.privateKey), publicKey: encode(r.publicKey) }
}

export function buildRawAddress(publicKeyBytes: Uint8Array): string {
  if (!publicKeyBytes || publicKeyBytes.length !== 32 || !(publicKeyBytes instanceof Uint8Array))
    throw new Error('Missing or invalid public key');

  const prefix = Uint8Array.from([1, networkByte]);
  const publicKeyHashPart = Uint8Array.from(hashChain(publicKeyBytes).slice(0, 20));

  const rawAddress = concatUint8Arrays(prefix, publicKeyHashPart);
  const addressHash = Uint8Array.from(hashChain(rawAddress).slice(0, 4));

  return encode(concatUint8Arrays(rawAddress, addressHash));
}

export function generateRandomUint32Array(length: number): Uint32Array {

  if (!length || length < 0) {
    throw new Error('Missing or invalid array length');
  }

  const a = secureRandom.randomUint8Array(length);
  const b = secureRandom.randomUint8Array(length);
  const result = new Uint32Array(length);

  for (let i = 0; i < length; i++) {
    const hash = converters.byteArrayToHexString(sha256(`${a[i]}${b[i]}`));
    const randomValue = parseInt(hash.slice(0, 13), 16);
    result.set([randomValue], i);
  }

  return result;
}


