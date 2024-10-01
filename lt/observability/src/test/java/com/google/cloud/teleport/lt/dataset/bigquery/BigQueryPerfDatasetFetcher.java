package com.google.cloud.teleport.lt.dataset.bigquery;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;

@AutoValue
public abstract class BigQueryPerfDatasetFetcher {

  abstract BigQueryResourceManager bigQueryResourceManager();

  abstract String query();

  public static BigQueryPerfDatasetFetcher.Builder builder() {
    return new AutoValue_BigQueryPerfDatasetFetcher.Builder();
  }

  public BigQueryPerfDataset fetch() {
    TableResult result = bigQueryResourceManager().runQuery(query());
    Schema schema = result.getSchema();
    List<PerfResultRow> perfResultRows = new ArrayList<>();
    for (FieldValueList row : result.iterateAll()) {
      PerfResultRow newPerfRow = new PerfResultRow(new HashMap<>());
      for (Field field : schema.getFields()) {
        String columnName = field.getName();
        FieldValue value = row.get(columnName);
        newPerfRow.row.put(columnName, getValue(field, value));
      }
      perfResultRows.add(newPerfRow);
    }
    return BigQueryPerfDataset.builder().setRows(perfResultRows).setQuery(query()).build();
  }

  public Object getValue(Field columnSchema, FieldValue value) {
    if (Attribute.REPEATED.equals(value.getAttribute())) {
      List<FieldValue> repeatedValue = value.getRepeatedValue();
      List<Object> returnVal = new ArrayList<>();
      for (int i = 0; i < repeatedValue.size(); ++i) {
        returnVal.add(getValue(columnSchema, repeatedValue.get(i)));
      }
      return returnVal;
    } else if (Attribute.RECORD.equals(value.getAttribute())) {
      FieldValueList recordValue = value.getRecordValue();
      FieldList subFields = columnSchema.getSubFields();
      Map<String, Object> returnVal = new HashMap<>();
      for (int i = 0; i < recordValue.size(); ++i) {
        returnVal.put(subFields.get(i).getName(), getValue(subFields.get(i), recordValue.get(i)));
      }
      return returnVal;
    } else {
      if (value.isNull()) {
        return null;
      }
      switch (columnSchema.getType().getStandardType()) {
        case BOOL:
          return value.getBooleanValue();
        case INT64:
          return value.getLongValue();
        case FLOAT64:
          return value.getDoubleValue();
        case STRING:
          return value.getStringValue();
        case TIMESTAMP:
          return value.getTimestampValue();
        default:
          return value.getValue();
      }
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract BigQueryPerfDatasetFetcher.Builder setBigQueryResourceManager(
        BigQueryResourceManager bigQueryResourceManager);

    public abstract BigQueryPerfDatasetFetcher.Builder setQuery(String query);

    abstract BigQueryPerfDatasetFetcher autoBuild();

    public BigQueryPerfDatasetFetcher build() {
      return autoBuild();
    }
  }
}
