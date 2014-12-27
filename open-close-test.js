var location

module.exports.setUp = function (test, testCommon) {
  location = testCommon.location()
  test('setUp', testCommon.setUp)
}

module.exports.populate = function (leveldown, test) {
  var db

  test('open', function (t) {
    db = leveldown(location)

    db.open(function (err) {
      t.notOk(err, 'no error')
      t.end()
    })
  })

  test('populate', function (t) {
    db.put('hello', 'world', function () {
      db.del('hello', function () {
        db.batch()
          .put('hello', 'world2')
          .del('hellz')
          .put('hellz', 'worldz')
          .write(t.end.bind(t))
      })
    })
  })

  test('close', function (t) {
    db.close(t.end.bind(t))
  })
}

module.exports.get = function (leveldown, test) {
  var db

  test('open', function (t) {
    db = leveldown(location)

    db.open(function (err) {
      t.notOk(err, 'no error')
      t.end()
    })
  })

  test('put some new data', function (t) {
    db.put('foo', 'bar', t.end.bind(t))
  })

  test('get', function (t) {
    db.get('hello', { asBuffer: false }, function (err, value) {
      t.notOk(err)
      t.equal(value, 'world2')
      db.get('hellz', { asBuffer: false }, function (err, value) {
        t.notOk(err)
        t.equal(value, 'worldz')
        db.get('foo', { asBuffer: false }, function (err, value) {
          t.notOk(err)
          t.equal(value, 'bar')
          t.end()
        })
      })
    })

  })

  test('close', function (t) {
    db.close(t.end.bind(t))
  })}

module.exports.tearDown = function (test, testCommon) {
  test('tearDown', testCommon.tearDown)
}

module.exports.all = function (leveldown, test, testCommon) {
  module.exports.setUp(test, testCommon)
  module.exports.populate(leveldown, test)
  module.exports.get(leveldown, test)
  module.exports.tearDown(test, testCommon)
}