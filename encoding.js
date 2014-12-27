var dz = require('dezalgo')
  , schema = require('protocol-buffers/require')('schema.proto')
  , snappy = require('snappy')

  , encodePut = function (obj, callback) {
      snappy.compress(obj.value, function (err, value) {
        if (err) return callback(err)

        var buffer = schema.Data.encode({
                key: obj.key
              , value: value
              , type: schema.TYPE.put
            })

        callback(null, buffer)
      })
    }
  , encodeDel = function (obj, callback) {
      return schema.Data.encode({
          key: obj.key
        , type: schema.TYPE.del
      })
    }

  , encode = function (obj, callback) {
      callback = dz(callback)

      if (obj.type === 'put')
        encodePut(obj, callback)
      else
        callback(null, encodeDel(obj))
    }
  , decode = function (buffer, options, callback) {

      if (!callback) {
        callback = options
        options = {}
      }

      callback = dz(callback)

      var obj = schema.Data.decode(buffer)

      obj.type = obj.type === schema.TYPE.put ? 'put' : 'del'

      if (obj.type === 'del') return callback(null, obj)

      snappy.uncompress(obj.value, options, function (err, value) {
        if (err) return callback(err)

        obj.value = value

        callback(null, obj)
      })
    }
  , decodeMeta = function (buffer) {
      var obj = schema.Data.decode(buffer)
      return {
            type: obj.type === schema.TYPE.put ? 'put' : 'del'
          , key: obj.key
      }
    }

module.exports = { encode: encode, decode: decode, decodeMeta: decodeMeta }