var fs = require('fs')
  , open = require('leveldown-open')
  , AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN

  , SimpleDOWN = function (location) {
      if (!(this instanceof SimpleDOWN))
        return new SimpleDOWN(location)

      AbstractLevelDOWN.call(this, location)
    }

require('util').inherits(SimpleDOWN, AbstractLevelDOWN)

SimpleDOWN.prototype._open = function (options, callback) {
  open(this.location, options, callback)
}

module.exports = SimpleDOWN
