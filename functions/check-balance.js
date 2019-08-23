const { getAccount } = require('../lib/account')

module.exports.handler = async (event) => {
  const { accountId } = event.pathParameters

  const { currentState } = await getAccount(accountId)

  return {
    statusCode: 200,
    body: JSON.stringify(currentState)
  }
}