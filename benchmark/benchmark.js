var medeadown = require('medeadown')
  , simpledown = require('../db')

  , medea = medeadown(__dirname + '/medea')
  , simple = simpledown(__dirname + '/simple')

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

require('co')(function *() {
  yield benchmark('medea.open', medea.open.bind(medea))
  yield benchmark('simple.open', simple.open.bind(simple))
  yield benchmark('medea.put small', put(medea, 'boop'))
  yield benchmark('medea.get small', get(medea))
  yield benchmark('medea.put large', put(medea, LARGE_VALUE))
  yield benchmark('medea.get large', get(medea))
  yield benchmark('simple.put small', put(simple, 'boop'))
  yield benchmark('simple.get small', get(simple))
  yield benchmark('simple.put large', put(simple, LARGE_VALUE))
  yield benchmark('simple.get large', get(simple))
})()
