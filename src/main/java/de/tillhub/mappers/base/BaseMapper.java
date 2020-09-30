package de.tillhub.mappers.base;

import com.google.api.services.bigquery.model.TableRow;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseMapper {
    private JSONArray jsonArray;
    public BaseMapper(String jsonText) {
        try {
            JSONObject jsonObject = new JSONObject(jsonText);
            this.jsonArray = (JSONArray) jsonObject.get("results");
        } catch (Throwable t) {
            System.out.println("An error occurred while parsing JSON");
            throw t;
        }
    }

    public TableRow[] readValues() {
        TableRow[] tableRowList = new TableRow[this.jsonArray.length()];
        for (int i = 0 ; i < this.jsonArray.length() ; i++) {
            JSONObject obj = (JSONObject) this.jsonArray.get(i);
            TableRow currTR = this.mapJsonToTableRow(obj);
            tableRowList[i] = currTR;
        }
        return tableRowList;
    }

    protected abstract TableRow mapJsonToTableRow(JSONObject obj);
}
