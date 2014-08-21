var fs = require('fs')

  , AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
  , Data = require('protocol-buffers/require')('schema.proto').Data
  , keydir = require('keydir')
  , open = require('leveldown-open')
  , series = require('run-series')
  , Skipfile = require('skipfile')

  , SimpleIterator = require('./iterator')

  , SimpleDOWN = function (location) {
      if (!(this instanceof SimpleDOWN))
        return new SimpleDOWN(location)

      AbstractLevelDOWN.call(this, location)
      this.filename = location + '/DATA'
      this.keys = {}
      this.keydir = keydir()
      this.position = 0
      this.skipfile = null
    }

  , encode = function (obj) {
      return obj.type === 'put' ?
        Data.encode({ key: obj.key, value: obj.value, deleted: false })
        :
        Data.encode({ key: obj.key, deleted: true })
    }
  , ensureBuffer = function (data) {
      if (data !== null && data !== undefined && !Buffer.isBuffer(data))
        data = new Buffer(String(data))

      return data
    }

require('util').inherits(SimpleDOWN, AbstractLevelDOWN)

SimpleDOWN.prototype._open = function (options, callback) {
  var self = this

  open(this.location, options, function (err) {
    if (err)
      return callback(err)

    Skipfile({ filename: self.filename }, function (err, skipfile) {
      self.skipfile = skipfile

      self._readDataFile(callback)
    })
  })
}

SimpleDOWN.prototype._readDataFile = function (callback) {
  var self = this
    , read = function (position) {
        if (position >= self.skipfile.size)
          return callback(null)

        self.skipfile.forward(position, function (err, seq, nextPosition, buffer) {
          if (err)
            return callback(err)

          var data = Data.decode(buffer)

          if (data.deleted) {
            delete self.keys[data.key]
            self.keydir.del(data.key)
          } else {
            self.keys[data.key] = position
            self.keydir.put(data.key)
          }

          read(nextPosition + 1)
        })
      }

  read(0)
}

SimpleDOWN.prototype._close = function (callback) {
  this.keys = undefined
  this.keydir = undefined
  this.skipfile.close(callback)
}

SimpleDOWN.prototype._put = function (key, value, options, callback) {
  key = ensureBuffer(key)
  value = ensureBuffer(value)

  var data = Data.encode({ key: key, value: value, deleted: false })
    , self = this
    , position = this.skipfile.size

  this.skipfile.append(data, function (err) {
    if (err)
      return callback(err)

    self.keys[key] = position
    self.keydir.put(key)

    callback()
  })
}

SimpleDOWN.prototype._del = function (key, options, callback) {
  if (this.keys[key] === undefined)
    return setImmediate(callback)

  if (!Buffer.isBuffer(key))
    key = new Buffer(String(key))

  var self = this
    , data = Data.encode({ key: key, deleted: true })

  this.skipfile.append(data, function (err) {
    if (err)
      return callback(err)

    delete self.keys[key]
    self.keydir.del(key)

    callback()
  })
}

SimpleDOWN.prototype._read = function (position, options, callback) {
  this.skipfile.forward(position, function (err, seq, pos, buffer) {
    if (err)
      return callback(err)

    var value = Data.decode(buffer).value

    if (options.asBuffer === false)
      value = value.toString()

    callback(null, value)
  })
}

SimpleDOWN.prototype._get = function (key, options, callback) {
  if (this.keys[key] === undefined)
    return setImmediate(callback.bind(null, new Error('NotFound:')))

  var position = this.keys[key]

  this._read(position, options, callback)
}

// TODO: make atomic - if I want to support this at all?
SimpleDOWN.prototype._batch = function (batch, options, callback) {
  var self = this

  series(
      batch.map(function (row) {
        return function (done) {
          if (row.type === 'put')
            self.put(row.key, row.value, done)
          else
            self.del(row.key, done)
        }
      })
    , callback
  )
}

SimpleDOWN.prototype._iterator = function (options) {
  return new SimpleIterator(this, options)
}

module.exports = SimpleDOWN
