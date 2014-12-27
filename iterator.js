var AbstractIterator = require('abstract-leveldown').AbstractIterator
  , dz = require('dezalgo')

  , SimpleIterator = function (db, options) {
      AbstractIterator.call(this, db)

      this.keyAsBuffer = options.keyAsBuffer !== false
      this.valueAsBuffer = options.valueAsBuffer !== false

      this.keydir = db.keydir.range(options)
      this.keys = this.keydir.reduce(function (keys, key) {
        keys[key] = db.keys[key]
        return keys
      }, {})
      this.idx = 0
    }

require('util').inherits(SimpleIterator, AbstractIterator)

SimpleIterator.prototype._next = function (callback) {
  callback = dz(callback)

  if (this.idx === this.keydir.length) return callback()

  var self = this
    , key = this.keydir[this.idx]
    , meta = this.keys[key]

  this.idx++

  this.db._read(meta, { asBuffer: this.valueAsBuffer }, function (err, value) {
    if (err) return callback(err)

    if (!self.keyAsBuffer)
      key = key.toString()

    callback(null, key, value)
  })
}

SimpleIterator.prototype._end = function (callback) {
  this.keydir = undefined
  this.keys = undefined
  callback()
}

module.exports = SimpleIterator