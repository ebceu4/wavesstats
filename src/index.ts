import { MongoClient } from 'mongodb'
import axios from 'axios'

async function main() {
  const db = await MongoClient.connect('mongodb://localhost:27017/')
  const collection = await db.db('waves').createCollection('blocks', { autoIndexId: true, })
  const h = (await axios.get('https://nodes.wavesnodes.com/blocks/height')).data.height
  const blocks = (await axios.get(`https://nodes.wavesnodes.com/blocks/seq/${h - 10}/${h}`)).data


  const result = await collection.insertMany(blocks.map((x: any) => {
    x._id = x.signature
    delete x.signature
    return x
  }))

  const r = await collection.aggregate([
    {
      $group: {
        _id: 0,
        data: { $push: "$transactions" }
      }
    },
    {
      $project: {
        data: {
          $reduce: {
            input: "$data",
            initialValue: [],
            in: { $setUnion: ["$$value", "$$this"] }
          }
        }
      }
    },
    { $unwind: "$data" },
    { $replaceRoot: { newRoot: "$data" } },
    { $match: { type: 4 } }
  ])

  //console.log(r.length)
  db.close()
}

main()










