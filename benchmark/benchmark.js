

var series = require('run-series')

  , medeadown = require('medeadown')
  , simpledown = require('../db')

  , medea = medeadown(__dirname + '/medea')
  , simple = simpledown(__dirname + '/simple')

  , getTasks = function (db) {
      var tasks = []

      for(var i = 0; i < 10000; ++i)(function (i) {
        tasks.push(function (done) {
          db.get('beep' + i, done)
        })
      })(i)
      return tasks
    }
  , putTasks = function (db, value) {
      var tasks = []

      for(var i = 0; i < 10000; ++i)(function (i) {
        tasks.push(function (done) {
          db.put('beep' + i, value, done)
        })
      })(i)
      return tasks
    }
  , run = function (name, tasks, done) {
      console.time(name)
      series(
          tasks
        , function () {
            console.timeEnd(name)
            done()
          }
      )
    }

series([
    medea.open.bind(medea)
  , simple.open.bind(simple)
  , function (done) {
      var tasks = putTasks(medea, 'boop')

      run('medea# put small', tasks, done)
    }
  , function (done) {
      var tasks = getTasks(medea)

      run('medea# get small', tasks, done)
    }
  , function (done) {
      var value = Array
            .apply(null, new Array(500))
            .map(function () { return 'boop' })
            .join('')
        , tasks = putTasks(medea, value)

      run('medea# put large', tasks, done)
    }
    , function (done) {
        var tasks = getTasks(medea)

        run('medea# get large', tasks, done)
      }
  , function (done) {
      var tasks = putTasks(simple, 'boop')

      run('simple# put small', tasks, done)
    }
  , function (done) {
      var tasks = getTasks(simple)

      run('simple# get small', tasks, done)
    }
  , function (done) {
      var value = Array
            .apply(null, new Array(500))
            .map(function () { return 'boop' })
            .join('')
        , tasks = putTasks(simple, value)

      run('simple# put large', tasks, done)
    }
  , function (done) {
      var tasks = getTasks(simple)

      run('simple# get large', tasks, done)
    }
])