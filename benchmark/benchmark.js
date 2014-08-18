var medeadown = require('medeadown')
  , simpledown = require('../db')

  , LARGE_VALUE = Array
      .apply(null, new Array(500))
      .map(function () { return 'boop' })
      .join('')

  , get = function (db) {
      return function * () {
        for(var i = 0; i < 10000; ++i) {
          yield function (done) {
            db.get('beep' + i, done)
          }
        }
      }
    }
  , put = function (db, value) {
      return function * () {
        for(var i = 0; i < 10000; ++i) {
          yield function (done) {
            db.put('beep' + i, value, done)
          }
        }
      }
    }
  , benchmark = function(name, yieldable) {
      return function * () {
        console.time(name)
        yield yieldable
        console.timeEnd(name)
      }
    }
  , dbs = [
        { name: 'medea', instance: medeadown(__dirname + '/medea') }
      , { name: 'simple', instance: simpledown(__dirname + '/simple') }
    ]

require('co')(function *() {
  var db
  for(var i = 0; i < dbs.length; ++i) {
    var db = dbs[i]
    yield benchmark(db.name + '.open', db.instance.open.bind(db.instance))
    yield benchmark(db.name + '.put small', put(db.instance, 'boop'))
    yield benchmark(db.name + '.get small', get(db.instance))
    yield benchmark(db.name + '.put large', put(db.instance, LARGE_VALUE))
    yield benchmark(db.name + '.get large', get(db.instance))
    yield benchmark(db.name + '.close', db.instance.close.bind(db.instance))
    console.log()
  }
})()
