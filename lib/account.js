const _ = require('lodash')
const https = require('https')

// see https://theburningmonk.com/2019/03/just-how-expensive-is-the-full-aws-sdk/
const DynamoDB = require('aws-sdk/clients/dynamodb')

// see https://theburningmonk.com/2019/02/lambda-optimization-tip-enable-http-keep-alive/
const sslAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 50,
  rejectUnauthorized: true,
})
sslAgent.setMaxListeners(0)  

const dynamodb = new DynamoDB.DocumentClient({
  service: new DynamoDB({
    httpOptions: {
      agent: sslAgent
    },
  }),
})

const { TABLE_NAME } = process.env

const EventType = {
  CREATED: 'CREATED',
  WITHDRAW: 'WITHDRAW',
  CREDIT: 'CREDIT',
  SNAPSHOT: 'SNAPSHOT'
}

const createAccount = async (id) => {
  const req = {
    TransactItems: []
  }

  req.TransactItems.push({
    Put: {
      TableName: TABLE_NAME,
      ConditionExpression: 'attribute_not_exists(Id)',
      Item: {
        Id: id,
        Version: 1,
        Type: EventType.CREATED,
        Timestamp: new Date().toJSON()
      }
    }
  })

  req.TransactItems.push({
    Put: {
      TableName: TABLE_NAME,
      ConditionExpression: 'attribute_not_exists(Version)',
      Item: {
        Id: id,
        Version: 2,
        Type: EventType.SNAPSHOT,
        Balance: 0.0,
        Timestamp: new Date().toJSON()
      }
    }
  })

  await dynamodb.transactWrite(req).promise()
}

const getAccount = async (id) => {
  const resp = await dynamodb.query({
    TableName: TABLE_NAME,
    KeyConditionExpression: 'Id = :id',
    ExpressionAttributeValues: { ':id': id },
    ConsistentRead: true,
    Limit: 10,
    ScanIndexForward: false // most recent events first
  }).promise()

  const events = resp.Items
  const snapshotIdx = events.findIndex(x => x.Type === 'SNAPSHOT')
  const snapshot = events[snapshotIdx]
  const eventsSinceSnapshot = _.reverse(_.range(0, snapshotIdx).map(idx => events[idx]))
  const currentState = _.reduce(eventsSinceSnapshot, (state, event) => {
    if (event.Type === EventType.WITHDRAW) {
      state.Balance -= event.Amount
      return state
    } else if (event.Type === EventType.CREDIT) {
      state.Balance += event.Amount
      return state
    } else {
      return state
    }
  }, { Id: snapshot.Id, Balance: snapshot.Balance })

  return {
    currentState,
    snapshot,
    eventsSinceSnapshot
  }
}

const addEvent = async (id, { type, amount, balance }, snapshot, eventsSinceSnapshot) => {
  const lastVersion = _.maxBy([ snapshot, ...eventsSinceSnapshot], 'Version').Version

  const req = {
    TransactItems: []
  }

  req.TransactItems.push({
    Put: {
      TableName: TABLE_NAME,
      ConditionExpression: 'attribute_not_exists(Version)',
      Item: {
        Id: id,
        Version: lastVersion + 1,
        Type: type,
        Amount: amount,
        Timestamp: new Date().toJSON()
      }
    }
  })

  // every tenth entry should be a snapshot
  // so if we just added event number 9, then we need to create a snapshot
  if (eventsSinceSnapshot.length >= 8) {
    req.TransactItems.push({
      Put: {
        TableName: TABLE_NAME,
        ConditionExpression: 'attribute_not_exists(Version)',
        Item: {
          Id: id,
          Version: lastVersion + 2,
          Type: EventType.SNAPSHOT,
          Balance: balance,
          Timestamp: new Date().toJSON()
        }
      }
    })
  }

  await dynamodb.transactWrite(req).promise()
}

module.exports = {
  EventType,
  createAccount,
  getAccount,
  addEvent
}