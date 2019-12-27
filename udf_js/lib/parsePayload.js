(function () {
  function binary2String(byteArray) {
      // converts a UTF-16 byte array to a string
      return String.fromCharCode.apply(String, byteArray);
  }

  /*  Input is either:
   *    - A gzipped UTF-8 byte array
   *    - A UTF-8 byte array
   *
   *  Outputs a string representation
   *  of the byte array (gunzipped if
   *  possible).
   */
  function decompress(input) {
    // BYTES are base64 encoded by BQ, so this needs to be decoded
    // Outputs a UTF-16 string
    var decodedData = atob(input);

    // convert UTF-16 string to byte array
    var compressedData = decodedData.split('').map(function(e) {
        return e.charCodeAt(0);
    });

    try {
      var gunzip = new Zlib.Gunzip(compressedData);

      // decompress returns bytes that need to be converted into a string
      var unzipped = gunzip.decompress();
      return binary2String(unzipped);
    } catch (err) {
      return binary2String(compressedData);
    }
  }

  function camelCaseToSnakeCase(input) {
    return input.split(/(?=[A-Z])/).join('_').toLowerCase();
  }

  function snakeCaseify(input) {
    if (typeof value === 'object' && value !== null) {
      result = {};
      for (const [key, value] of Object.entries(input)) {
        result[camelCaseToSnakeCase(key)] = snakeCaseify(value);
      }
      return result;
    } else {
      return input;
    }
  }

  parsePayload = function (input) {
    return snakeCaseify(JSON.parse(decompress(input)));
  };

}());
