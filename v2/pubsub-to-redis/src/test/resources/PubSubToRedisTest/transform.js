/**
 * A good transform function.
 * @param {string} inJson
 * @return {string} outJson
 */
function transform(inJson) {
  var obj = JSON.parse(inJson);
  obj.transformedBy = "UDF";
  obj.timestamp = new Date().toISOString();
  return JSON.stringify(obj);
}

/**
 * A bad transform function that throws an error.
 * @param {string} inJson
 * @return {string} outJson
 */
function transformBad(inJson) {
  throw new Error("Intentional error for testing");
}
