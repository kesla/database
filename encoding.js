var schema = require('protocol-buffers/require')('schema.proto')

  , encode = function (obj) {
      return schema.Data.encode({
          key: obj.key
        , value: obj.value
        , type: schema.TYPE[obj.type]
      })
    }
  , decode = function (buffer) {
      var obj = schema.Data.decode(buffer)

      obj.type = obj.type === schema.TYPE.put ? 'put' : 'del'
      return obj
    }

module.exports = { encode: encode, decode: decode }