const { getAccount, addEvent } = require('../lib/account')

module.exports.handler = async (event) => {
  console.log(JSON.stringify(event))

  const { accountId } = event.pathParameters
  const { amount } = JSON.parse(event.body)

  const { currentState, snapshot, eventsSinceSnapshot } = await getAccount(accountId)

  const balance = currentState.Balance + amount
  const newEvent = {
    type: 'CREDIT',
    amount: amount,
    balance
  }

  await addEvent(accountId, newEvent, snapshot, eventsSinceSnapshot)

  return {
    statusCode: 200,
    body: JSON.stringify({ accountId, balance })
  }
}