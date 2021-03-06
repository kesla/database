var AbstractBatch = require('abstract-leveldown/abstract-chained-batch')
  , collect = require('collect-stream')
  , Orderable = require('orderable')
  , varint = require('varint')

  , encoding = require('./encoding')

  , SimpleBatch = function (db) {
      AbstractBatch.call(this, db)
      this._stream = Orderable()
      this._index = 0
    }
  , put = function (key, value, index, stream) {

      key = Buffer.isBuffer(key) ? key : new Buffer(String(key))
      value = (Buffer.isBuffer(value) || typeof(value) === 'string')? value : String(value)

      encoding.encode({ key: key, value: value, type: 'put' }, function (err, buffer) {
        if (err) stream.emit('error', err)
        else stream.set(index, { key: key, type: 'put', buffer: buffer })
      })
    }
  , del = function (key, index, stream) {
      key = Buffer.isBuffer(key) ? key : new Buffer(String(key))

      encoding.encode({ key: key, type: 'del' }, function (err, buffer) {
        if (err) stream.emit('error', err)
        else stream.set(index, { key: key, type: 'del', buffer: buffer })
      })
    }

require('util').inherits(SimpleBatch, AbstractBatch)

SimpleBatch.prototype._put = function (key, _value) {
  put(key, _value, this._index, this._stream)
  this._index = this._index + 1
}

SimpleBatch.prototype._del = function (key) {
  del(key, this._index, this._stream)
  this._index = this._index + 1
}

SimpleBatch.prototype._clear = function () {
  this._stream = Orderable()
  this._index = 0
}

SimpleBatch.prototype._write = function (callback) {
  var self = this
    , keysDelta = {}

  this._stream.set(this._index, null)

  collect(this._stream, function (err, batch) {

    var buffers = batch.map(function (row) {
          var data = row.buffer
            , size = varint.encodingLength(data.length)
            , buffer = new Buffer(size + data.length)
            , oldPosition = self._db.position

          self._db.position += buffer.length

          varint.encode(data.length, buffer)
          data.copy(buffer, size)
          if (row.type === 'put')
            keysDelta[row.key] = {
                position: oldPosition + size
              , size: data.length
            }

          return buffer
        })

    self._db.stream.write(Buffer.concat(buffers), function (err) {
      if (err) return callback(err)

      batch.forEach(function (row) {
        if (row.type === 'put') {
          self._db.keys[row.key] = keysDelta[row.key]
          self._db.keydir.put(row.key)
        } else {
          delete self._db.keys[row.key]
          self._db.keydir.del(row.key)
        }
      })

      callback()
    })
  })
}

module.exports = SimpleBatch
