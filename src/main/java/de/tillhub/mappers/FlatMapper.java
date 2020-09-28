package de.tillhub.mappers;

import com.google.api.services.bigquery.model.TableRow;
import de.tillhub.mappers.base.BaseMapper;
import org.json.JSONObject;

import java.util.Iterator;

public class FlatMapper extends BaseMapper {

    public FlatMapper(String jsonText) {
        super(jsonText);
    }

    @Override
    protected TableRow mapJsonToTableRow(JSONObject obj) {
        TableRow row = new TableRow();
        Iterator<String> keys = obj.keys();

        while(keys.hasNext()) {
            String key = keys.next();
            row.set(key, obj.get(key));
        }
        return row;
    }
}
