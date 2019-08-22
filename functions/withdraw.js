const { getAccount, addEvent } = require('../lib/account')

module.exports.handler = async (event) => {
  const { accountId } = event.pathParameters
  const { amount } = JSON.parse(event.body)

  const { currentState, snapshot, eventsSinceSnapshot } = await getAccount(accountId)

  if (currentState.Balance < amount) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        message: 'insufficient balance'
      })
    }
  }

  const balance = currentState.Balance - amount
  const newEvent = {    
    type: 'WITHDRAW',
    amount: amount,
    balance
  }

  await addEvent(accountId, newEvent, snapshot, eventsSinceSnapshot)

  return {
    statusCode: 200,
    body: JSON.stringify({ accountId, balance })
  }
}