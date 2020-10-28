package de.tillhub.mappers;

import com.google.api.services.bigquery.model.TableRow;
import de.tillhub.mappers.base.BaseMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class FlatMapper extends BaseMapper {

    private static final Logger LOG = LoggerFactory.getLogger(FlatMapper.class);

    public FlatMapper(String jsonText) {
        super(jsonText);
    }

    @Override
    protected TableRow mapJsonToTableRow(JSONObject obj) throws IOException {
        LOG.info(FlatMapper.class.getCanonicalName() + ":: received input JSON " + obj.toString() );
        TableRow row = new TableRow();

        Map<String, Object> javaElementsFromJSON = this.mapElementsRecursive(obj);
        for (Map.Entry<String, Object> entry : javaElementsFromJSON.entrySet()) {
            row.set(entry.getKey(), entry.getValue());
        }
        LOG.info(FlatMapper.class.getCanonicalName() + ":: result Row " + row.toPrettyString() );
        return row;
    }

    public Map<String, Object> mapElementsRecursive(JSONObject currObject) throws JSONException {
        Map<String, Object> elemMap = new HashMap<String, Object>();
        List<Object> elemMapArr = null;

        Iterator<?> keys = currObject.keys();
        while(keys.hasNext() ){
            // handle first records and repeating records:
            String key = (String)keys.next();
            if(currObject.get(key) instanceof JSONObject){
                LOG.info(FlatMapper.class.getCanonicalName() + ":: " + key + " is Object");
                // API-224: should handle also empty objects ( "{}" }
                if(((JSONObject) currObject.get(key)).length() == 0) {
                    LOG.warn("Key " + key + " is an empty object. Resolving to null");
                    elemMap.put(key, null);
                } else {
                    elemMap.put(key, mapElementsRecursive((JSONObject)currObject.get(key)));
                }
            } else if(currObject.get(key) instanceof JSONArray) {
                LOG.info(FlatMapper.class.getCanonicalName() + ":: " + key + " is Array");
                elemMapArr = new ArrayList<Object>();
                JSONArray jArray = (JSONArray)currObject.get(key);
                for(int i = 0; i < jArray.length(); i++) {
                    if(jArray.get(i) instanceof JSONObject || jArray.get(i) instanceof JSONArray){
                        elemMapArr.add(mapElementsRecursive((JSONObject)jArray.get(i)));
                    } else {
                        elemMapArr.add(jArray.get(i));
                    }
                }
                elemMap.put(key, elemMapArr);
            } else {
                // maps specific leaf elements according to their type:
                Object value = currObject.get(key);
                if (value instanceof Integer || value instanceof Long) {
                    LOG.info(FlatMapper.class.getCanonicalName() + ":: " + key + " is Integer");
                    long intToUse = ((Number)value).longValue();
                    elemMap.put(key, Long.valueOf(intToUse));
                } else if (value instanceof Boolean) {
                    LOG.info(FlatMapper.class.getCanonicalName() + ":: " + key + " is Boolean");
                    elemMap.put(key, value);
                } else if (value instanceof Float || value instanceof Double) {
                    LOG.info(FlatMapper.class.getCanonicalName() + ":: " + key + " is Float");
                    double floatToUse = ((Number)value).doubleValue();
                    elemMap.put(key, Double.valueOf(floatToUse));
                } else if (JSONObject.NULL.equals(value)) {
                    LOG.info(FlatMapper.class.getCanonicalName() + ":: " + key + " is Null");
                    elemMap.put(key, null);
                } else {
                    String stringToUse = currObject.getString(key);
                    // there's no way in JS to pass float. therefore the token '@@@FLOAT' denotes
                    // a float string prefixing it.
                    if (stringToUse.contains("@@@FLOAT")) {
                        String[] split = stringToUse.split("@@@FLOAT");
                        double doubleVal = Double.parseDouble(split[0]);
                        LOG.info(FlatMapper.class.getCanonicalName() + ":: " + key + " is interpreted as Float " + doubleVal);
                        elemMap.put(key, Double.valueOf(doubleVal));
                    } else {
                        LOG.info(FlatMapper.class.getCanonicalName() + ":: " + key + " is String");
                        elemMap.put(key, stringToUse);
                    }
                }
            }
        }
        return elemMap;
    }
}
