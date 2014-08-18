var fs = require('fs')

  , AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
  , appendStream = require('append-stream')
  , Data = require('protocol-buffers/require')('schema.proto').Data
  , open = require('leveldown-open')
  , varint = require('varint')

  , SimpleDOWN = function (location) {
      if (!(this instanceof SimpleDOWN))
        return new SimpleDOWN(location)

      AbstractLevelDOWN.call(this, location)
      this.keys = {}
      this.stream = null
      this.position = 0
      this.fd = null
    }

require('util').inherits(SimpleDOWN, AbstractLevelDOWN)

SimpleDOWN.prototype._open = function (options, callback) {
  var self = this

  open(this.location, options, function (err) {
    if (err)
      return callback(err)

    appendStream(self.location + '/DATA', function (err, stream) {
      if (err)
        return callback(err)

      self.stream = stream

      fs.open(self.location + '/DATA', 'r', function (err, fd) {
        if (err)
          return callback(err)

        self.fd = fd
        callback()
      })
    })
  })
}

SimpleDOWN.prototype._close = function (callback) {
  this.stream.end()
  fs.close(this.fd, callback)
}

SimpleDOWN.prototype._append = function (data, callback) {
  var self = this
    , size = varint.encodingLength(data.length)
    , buffer = new Buffer(size + data.length)
    , oldPosition = this.position

  this.position += buffer.length

  varint.encode(data.length, buffer)
  data.copy(buffer, size)

  this.stream.write(buffer, function (err) {
    if (err)
      return callback(err)

    callback(null, oldPosition, size)
  })
}

SimpleDOWN.prototype._put = function (key, value, options, callback) {
  if (!Buffer.isBuffer(key))
    key = new Buffer(String(key))

  if (!Buffer.isBuffer(value))
    value = new Buffer(String(value))

  var data = Data.encode({ key: key, value: value, deleted: false })
    , self = this

  this._append(data, function (err, oldPosition, size) {
    if (err)
      return callback(err)

    self.keys[key] = {
        position: oldPosition + size
      , size: data.length
    }

    callback()
  })
}

SimpleDOWN.prototype._del = function (key, options, callback) {
  if (!(this.keys[key]))
    return setImmediate(callback)

  if (!Buffer.isBuffer(key))
    key = new Buffer(String(key))

  var self = this
    , data = Data.encode({ key: key, deleted: true })

  this._append(data, function (err) {
    if (err)
      return callback(err)

    delete self.keys[key]
    callback()
  })
}

SimpleDOWN.prototype._get = function (key, options, callback) {
  if (!(this.keys[key]))
    return setImmediate(callback.bind(null, new Error('NotFound:')))

  var meta = this.keys[key]
    , buffer = new Buffer(meta.size)

  fs.read(this.fd, buffer, 0, buffer.length, meta.position, function (err) {
    if (err)
      return callback(err)

    var value = Data.decode(buffer).value

    if (options.asBuffer === false)
      value = value.toString()

    callback(null, value)
  })

}

module.exports = SimpleDOWN
