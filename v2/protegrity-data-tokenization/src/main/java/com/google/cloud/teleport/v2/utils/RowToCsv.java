package com.google.cloud.teleport.v2.utils;

import java.util.stream.Collectors;
import org.apache.beam.sdk.values.Row;

/**
 * The {@link RowToCsv} class to convert Beam Rows into strings in CSV format.
 */
public class RowToCsv {

  private final String csvDelimiter;

  public RowToCsv(String csvDelimiter) {
    this.csvDelimiter = csvDelimiter;
  }

  public String getCsvFromRow(Row row) {
    return row.getValues()
        .stream()
        .map(item -> item == null ? "null" : item)
        .map(Object::toString)
        .collect(Collectors.joining(csvDelimiter));
  }
}
