const uuid = require('uuid/v4')
const { createAccount } = require('../lib/account')

module.exports.handler = async (event) => {
  console.log(JSON.stringify(event))
  
  const accountId = uuid()

  await createAccount(accountId)

  return {
    statusCode: 200,
    body: JSON.stringify({ accountId })
  }
}