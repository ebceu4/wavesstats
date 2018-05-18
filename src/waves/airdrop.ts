import * as fs from 'fs'
import * as readline from 'readline'
import { airdrop } from './send';
import { config } from '../config';

const assetId = 'FNFEwvwXEW2w8bzCFhMFL51xXkCt8xAB8gzxqmYRFVx9' //JOHN
//const assetId = undefined
const amount = 200000000

const file = readline.createInterface({
  input: fs.createReadStream(process.cwd() + '/' + process.argv[2]),
  output: fs.createWriteStream(process.cwd() + '/' + process.argv[2] + "_success"),
})

var addresses: any[] = []

var failed = 0
var success = 0

const flush = (self: any) => {
  const a = addresses
  addresses = []
  const w = a.join('\n') + '\n'
  airdrop(assetId, amount, a, config.seed)
    .then(x => {
      self.output.write(w)
      success += a.length
      console.log('Success wallets: ' + success)
    })
    .catch(x => {
      console.log('Failed wallets: ' + failed)
      fs.appendFileSync(process.cwd() + '/' + process.argv[2] + '_failed', w)
      airdrop(assetId, amount, a, config.seed).catch(_ => { })
        .then(_ => { console.log('retry complete!') })
    })
}

file.on('line', function (address: string) {
  addresses.push(address)
  if (addresses.length == 100) {
    flush(this)
  }
})

file.on('close', function () {
  flush(this)
})