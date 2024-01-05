package com.keap.dataflow.flagshipevents;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;

public class TableContent implements Serializable {
    private Object tableRow;
    private Object tableSchema;
    private Object tableName;
    private Object projectAndDataset;

    public TableRow getTableRow() {
        return (TableRow)tableRow;
    }

    public void setTableRow(TableRow tableRow) {
        this.tableRow = tableRow;
    }

    public TableSchema getTableSchema() {
        return (TableSchema)tableSchema;
    }

    public void setTableSchema(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return (String)tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getProjectAndDataset() {
        return (String)projectAndDataset;
    }

    public void setProjectAndDataset(String projectAndDataset) {
        this.projectAndDataset = projectAndDataset;
    }
}
