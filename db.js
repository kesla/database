var fs = require('fs')

  , AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
  , appendStream = require('append-stream')
  , Data = require('protocol-buffers/require')('schema.proto').Data
  , keydir = require('keydir')
  , open = require('leveldown-open')
  , snappy = require('snappy')
  , varint = require('varint')

  , ChainedBatch = require('./batch')
  , SimpleIterator = require('./iterator')

  , SimpleDOWN = function (location) {
      if (!(this instanceof SimpleDOWN))
        return new SimpleDOWN(location)

      AbstractLevelDOWN.call(this, location)
      this.filename = location + '/DATA'
      this.keys = {}
      this.keydir = keydir()
      this.stream = null
      this.position = 0
      this.fd = null
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

    appendStream(self.filename, { flags: 'a+' }, function (err, stream) {
      if (err)
        return callback(err)

      self._readDataFile(function (err) {
        if (err)
          return callback(err)

        self.stream = stream
        self.fd = stream.fd
        callback()
      })
    })
  })
}

SimpleDOWN.prototype._readDataFile = function (callback) {
  var self = this

  fs.readFile(this.filename, function (err, file) {
    if (err)
      return callback(err)

    var position = 0
      , length
      , data

    while(position < file.length) {
      length = varint.decode(file, position)

      position += varint.decode.bytes

      data = Data.decode(file.slice(position, position + length))
      if (data.deleted) {
        delete self.keys[data.key]
        self.keydir.del(data.key)
      } else {
        self.keys[data.key] = {
            position: position
          , size: length
        }
        self.keydir.put(data.key)
      }
      position += length
    }

    self.position = file.length

    callback()
  })
}

SimpleDOWN.prototype._close = function (callback) {
  this.stream.end()
  fs.close(this.fd, callback)
  this.keys = undefined
  this.keydir = undefined
}

SimpleDOWN.prototype._append = function (data, callback) {
  var size = varint.encodingLength(data.length)
    , buffer = new Buffer(size + data.length)
    , oldPosition = this.position

  this.position += buffer.length

  varint.encode(data.length, buffer)
  data.copy(buffer, size)

  this.stream.write(buffer, function (err) {
    if (err)
      return callback(err)

    callback(null, oldPosition + size)
  })
}

SimpleDOWN.prototype._put = function (key, _value, options, callback) {
  var self = this

  key = ensureBuffer(key)
  _value = ensureBuffer(_value)

  snappy.compress(_value, function (err, value) {
    if (err)
      return callback(err)

    var data = Data.encode({ key: key, value: value, deleted: false })

    self._append(data, function (err, position) {
      if (err)
        return callback(err)

      self.keys[key] = {
          position: position
        , size: data.length
      }
      self.keydir.put(key)

      callback()
    })
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
    self.keydir.del(key)

    callback()
  })
}

SimpleDOWN.prototype._read = function (meta, options, callback) {
  var buffer = new Buffer(meta.size)

  fs.read(this.fd, buffer, 0, buffer.length, meta.position, function (err) {
    if (err)
      return callback(err)

    var _value = Data.decode(buffer).value

    snappy.uncompress(_value, options, function (err, value) {
      if (err)
        return callback(err)

      callback(null, value)
    })
  })
}

SimpleDOWN.prototype._get = function (key, options, callback) {
  if (!(this.keys[key]))
    return setImmediate(callback.bind(null, new Error('NotFound:')))

  var meta = this.keys[key]

  this._read(meta, options, callback)
}

SimpleDOWN.prototype._chainedBatch = function () {
  return new ChainedBatch(this)
}

SimpleDOWN.prototype._batch = function (batch, options, callback) {
  var chainedBatch = this._chainedBatch()

  batch.forEach(function (row) {
    if (row.type === 'del') {
      chainedBatch.del(row.key)
    } else {
      chainedBatch.put(row.key, row.value)
    }
  })

  chainedBatch.write(callback)
}

SimpleDOWN.prototype._iterator = function (options) {
  return new SimpleIterator(this, options)
}

module.exports = SimpleDOWN
