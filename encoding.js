var schema = require('protocol-buffers/require')('schema.proto')
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
      setImmediate(function () {
        var buffer = schema.Data.encode({
                key: obj.key
              , type: schema.TYPE.del
            })
        callback(null, buffer)
      })
    }

  , encode = function (obj, callback) {
      if (obj.type === 'put')
        encodePut(obj, callback)
      else
        encodeDel(obj, callback)
    }
  , decode = function (buffer, options, callback) {

      if (!callback) {
        callback = options
        options = {}
      }

      var obj = schema.Data.decode(buffer)

      obj.type = obj.type === schema.TYPE.put ? 'put' : 'del'

      if (obj.type === 'del') {
        setImmediate(function () {
          callback(null, obj)
        })
      } else {
        snappy.uncompress(obj.value, options, function (err, value) {
          if (err) return callback(err)

          obj.value = value

          callback(null, obj)
        })
      }
    }
  , decodeMeta = function (buffer) {
      var obj = schema.Data.decode(buffer)
      return {
            type: obj.type === schema.TYPE.put ? 'put' : 'del'
          , key: obj.key
      }
    }

module.exports = { encode: encode, decode: decode, decodeMeta: decodeMeta }