/**
 * Unwraps Splunk HEC-formatted payloads to prevent event nesting.
 * 
 * This function detects if the input is a HEC-formatted JSON object
 * (containing an "event" field) and extracts just the event content.
 * If the input is not HEC-formatted, it returns it unchanged.
 * 
 * This prevents event nesting when replaying messages from deadletter queue.
 * 
 * @param {string} inJson - Input JSON string (may be HEC-formatted or regular)
 * @return {string} - Unwrapped payload
 */
function transform(inJson) {
  try {
    var obj = JSON.parse(inJson);
    
    // Check if this looks like HEC format
    // HEC format has "event" field and may have other metadata fields
    if (obj.hasOwnProperty('event')) {
      
      // List of known HEC metadata fields
      var hecFields = ['event', 'time', 'host', 'index', 'source', 'sourcetype', 'fields'];
      var objKeys = Object.keys(obj);
      
      // Check if all keys are HEC fields (strong indicator it's HEC format)
      var isHecFormat = objKeys.every(function(key) {
        return hecFields.indexOf(key) !== -1;
      });
      
      // If it looks like HEC format, extract the event
      if (isHecFormat) {
        var event = obj.event;
        
        // The event could be a string or an object
        if (typeof event === 'string') {
          return event;
        } else {
          // If event is an object/array, stringify it
          return JSON.stringify(event);
        }
      }
    }
    
    // Not HEC format, return as-is
    return inJson;
    
  } catch (e) {
    // If parsing fails, it's not JSON - return as-is
    return inJson;
  }
}
