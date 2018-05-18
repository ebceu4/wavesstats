import { sendDataTransaction } from "./send";
import * as Crypto from './crypto'
import { IDataEntry } from "./transactions";

const sendData = async (seed: string, data: IDataEntry[]) => {
  const keys = Crypto.createKeyPair(seed)
  try {
    const r = await sendDataTransaction({
      senderPublicKey: keys.publicKey,
      version: 1,
      data,
      fee: 100000,
      timestamp: Date.now()
    }, seed)
  }
  catch (ex) {
    console.log(ex)
  }
}

async function demo() {

  let total = 0
  let current = 0

  Array(1000).fill(0).forEach(async _ => {
    total += 1
    await sendData('Supplier02', [
      { key: 'param1', type: 'integer', value: 1 },
      { key: 'param2', type: 'binary', value: '3Es4b2e8tHSbLB6eMhQh7z3fav4mkukjebQ' },
      { key: 'param3', type: 'boolean', value: true },
    ])
    current += 1
    console.log(`${current}/${total}`)
  })
}

demo()