var test       = require('tape')
  , testCommon = require('abstract-leveldown/testCommon')
  , factory  = require('./db')
  , testBuffer = require('fs').readFileSync(__filename)
  , db

/*** compatibility with basic LevelDOWN API ***/

require('abstract-leveldown/abstract/leveldown-test').args(factory, test, testCommon)

require('abstract-leveldown/abstract/open-test').all(factory, test, testCommon)

require('abstract-leveldown/abstract/del-test').all(factory, test, testCommon)

require('abstract-leveldown/abstract/get-test').all(factory, test, testCommon)

require('abstract-leveldown/abstract/put-test').all(factory, test, testCommon)

require('abstract-leveldown/abstract/put-get-del-test').all(factory, test, testCommon, testBuffer)

require('abstract-leveldown/abstract/batch-test').all(factory, test, testCommon)
require('abstract-leveldown/abstract/iterator-test').all(factory, test, testCommon)

require('abstract-leveldown/abstract/chained-batch-test').all(factory, test, testCommon)

require('abstract-leveldown/abstract/close-test').close(factory, test, testCommon)

require('abstract-leveldown/abstract/ranges-test').all(factory, test, testCommon)
