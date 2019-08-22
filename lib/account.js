const _ = require('lodash')
const AWS = require('aws-sdk')
const dynamodb = new AWS.DynamoDB.DocumentClient()

const { TABLE_NAME } = process.env

const createAccount = async (id) => {
  await dynamodb.put({
    TableName: TABLE_NAME,
    ConditionExpression: 'attribute_not_exists(Id)',
    Item: {
      Id: id,
      Version: 1,
      Type: 'SNAPSHOT',
      Balance: 0.0,
      Timestamp: new Date().toJSON()
    }
  }).promise()
}

const getAccount = async (id) => {
  const resp = await dynamodb.query({
    TableName: TABLE_NAME,
    KeyConditionExpression: 'Id = :id',
    ExpressionAttributeValues: { ':id': id },
    Limit: 10,
    ScanIndexForward: false // most recent events first
  }).promise()

  const events = resp.Items
  const snapshotIdx = events.findIndex(x => x.Type === 'SNAPSHOT')
  const snapshot = events[snapshotIdx]
  const eventsSinceSnapshot = _.reverse(_.range(0, snapshotIdx).map(idx => events[idx]))
  const currentState = _.reduce(eventsSinceSnapshot, (state, event) => {
    if (event.Type === 'WITHDRAW') {
      state.Balance -= event.Amount
      return state
    } else {
      state.Balance += event.Amount
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
          Type: 'SNAPSHOT',
          Balance: balance,
          Timestamp: new Date().toJSON()
        }
      }
    })
  }

  try {
    await dynamodb.transactWrite(req).promise()
  } catch (err) {
    throw err
  }  
}

module.exports = {
  createAccount,
  getAccount,
  addEvent
}