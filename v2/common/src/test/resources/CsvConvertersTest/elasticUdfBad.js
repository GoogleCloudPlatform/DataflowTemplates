/**
 * This function splits a csv and turns it into a pre-defined JSON.
 * @param {string} line is a line from TextIO.
 * @return {JSON} a JSON created after parsing the line.
 */
function transform(line) {
  try {
    var split = JSON.parse(line);
      var obj = new Object();
      obj.id = split[0];
      obj.state = split[1];
      obj.price = parseFloat(split[2]);
      var jsonString = JSON.stringify(obj);
      return jsonString;

  } catch (e) {
    return false;
  }

}
