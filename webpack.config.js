module.exports = {
  entry: {
    'functions/check-balance': './functions/check-balance.js',
    'functions/create-account': './functions/create-account.js',
    'functions/credit': './functions/credit.js',
    'functions/withdraw': './functions/withdraw.js',
  },
  mode: 'production',
  target: 'node'
}