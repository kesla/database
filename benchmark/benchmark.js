var rimraf = require('rimraf')

  , medeadown = require('medeadown')
  , simpledown = require('../db')

  , LARGE_VALUE = Array
      .apply(null, new Array(500))
      .map(function () { return 'boop' })
      .join('')

    , put = function (db, value) {
        return function * () {
          for(var i = 0; i < 10000; ++i) {
            yield function (done) {
              db.put('beep' + i, value, done)
            }
          }
        }
      }
  , get = function (db) {
      return function * () {
        for(var i = 0; i < 10000; ++i) {
          yield function (done) {
            db.get('beep' + i, done)
          }
        }
      }
    }
  , iterator = function (db, options) {
      var iterator = db.iterator(options)
      return function * () {
        while((data = yield iterator.next.bind(iterator)));
        yield iterator.end.bind(iterator)
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
  , inputs = [
        { name: 'small', value: 'boop' }
      , { name: 'large', value: LARGE_VALUE }
    ]

require('co')(function *() {
  var db
    , input

  for(var i = 0; i < dbs.length; ++i) {
    var db = dbs[i]
    rimraf.sync(__dirname + '/' + db.name)
    yield benchmark(db.name + '.open', db.instance.open.bind(db.instance))
    for(var j = 0; j < inputs.length; ++j) {
      var input = inputs[j]
      console.log(input.name + ':')
      yield benchmark(db.name + '.put', put(db.instance, input.value))
      yield benchmark(db.name + '.get', get(db.instance))
      yield benchmark(db.name + '.iterator (100%)', iterator(db.instance))
      yield benchmark(db.name + '.iterator ( 10%)', iterator(db.instance, { limit: 1000 }))
      yield benchmark(db.name + '.iterator (  1%)', iterator(db.instance, { limit: 100 }))
    }
    yield benchmark(db.name + '.close', db.instance.close.bind(db.instance))
    console.log()
  }
})()
