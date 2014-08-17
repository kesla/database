var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN

  , SimpleDOWN = function (location) {
      if (!(this instanceof SimpleDOWN))
        return new SimpleDOWN(location)

      AbstractLevelDOWN.call(this, location)
    }

require('util').inherits(SimpleDOWN, AbstractLevelDOWN)

module.exports = SimpleDOWN
