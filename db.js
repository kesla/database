var fs = require('fs')

  , AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
  , appendStream = require('append-stream')
  , dz = require('dezalgo')
  , keydir = require('keydir')
  , open = require('leveldown-open')
  , snappy = require('snappy')
  , varint = require('varint')

  , ChainedBatch = require('./batch')
  , encoding = require('./encoding')
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

      data = encoding.decodeMeta(file.slice(position, position + length))
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

SimpleDOWN.prototype._put = function (key, value, options, callback) {
  var self = this
    , obj = { type: 'put', key: ensureBuffer(key), value: ensureBuffer(value) }

  encoding.encode(obj, function (err, buffer) {
    if (err)
      return callback(err)

    self._append(buffer, function (err, position) {
      if (err)
        return callback(err)

      self.keys[obj.key] = {
          position: position
        , size: buffer.length
      }
      self.keydir.put(obj.key)

      callback()
    })
  })
}

SimpleDOWN.prototype._del = function (key, options, callback) {
  callback = dz(callback)

  if (!(this.keys[key]))
    return callback()

  var self = this
    , obj = { type: 'del', key: ensureBuffer(key) }

  encoding.encode(obj, function (err, buffer) {
    if (err)
      return callback(err)

    self._append(buffer, function (err) {
      if (err)
        return callback(err)

      delete self.keys[obj.key]
      self.keydir.del(obj.key)

      callback()
    })
  })
}

SimpleDOWN.prototype._read = function (meta, options, callback) {
  var buffer = new Buffer(meta.size)

  fs.read(this.fd, buffer, 0, buffer.length, meta.position, function (err) {
    if (err)
      return callback(err)

    encoding.decode(buffer, options, function (err, obj) {
      if (err) return callback(err)
      callback(null, obj.value)
    })
  })
}

SimpleDOWN.prototype._get = function (key, options, callback) {
  callback = dz(callback)

  if (!(this.keys[key]))
    return callback(new Error('NotFound:'))

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
