import { MongoClient } from 'mongodb'
import { config } from './config'
import axios from 'axios'
import { Block } from './api/interfaces'
import * as J from 'json-bigint'

async function getBlocks(): Promise<Block[]> {
  const h = (await axios.get('https://nodes.wavesnodes.com/blocks/height')).data.height
  return (await axios.get(`https://nodes.wavesnodes.com/blocks/seq/${h - 10}/${h}`, {
    transformResponse: (data) => {
      const blocks: any[] = J.parse(data)
      return blocks.map(b => {
        b._id = b.signature
        delete b.signature
        return b
      })
    }
  })).data
}

async function fetchBlocks() {
  const db = await MongoClient.connect(config.mongoUri)
  const blocksTable = await db.db('waves').createCollection('blocks')
  const blocks = await getBlocks()

  blocks.forEach(block => {
    blocksTable.update(
      { "_id": block._id },
      { $set: block },
      { upsert: true }
    )
  })
  db.close()
}

async function main() {
  await fetchBlocks()
}

main()









