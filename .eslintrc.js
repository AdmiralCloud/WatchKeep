const config = {
  'env': {
    'commonjs': true,
    'es6': true,
    'node': true
  },
  'extends': 'eslint:recommended',
  "rules": {
    "space-before-function-paren": 0,
    "no-extra-semi": 0,
    "object-curly-spacing": ["error", "always"],
    "brace-style": ["error", "stroustrup", { "allowSingleLine": true }],
    "no-useless-escape": 0,
    "standard/no-callback-literal": 0,
    "new-cap": 0
  },
  globals: {
    describe: true,
    it: true,
    before: true,
    after: true
  },
  'parserOptions': {
    'ecmaVersion': 2018
  }
}

module.exports = config
