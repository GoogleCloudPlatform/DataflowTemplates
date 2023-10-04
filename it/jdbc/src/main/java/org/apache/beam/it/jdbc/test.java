package org.apache.beam.it.jdbc;

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class test {
  public static void main(String[] args) {
    JDBCResourceManager rm = MySQLResourceManager.builder("test").build();

    Map<String, String> columns = new LinkedHashMap<>();
    columns.put("id", "INTEGER");
    columns.put("first", "VARCHAR(32)");
    columns.put("last", "VARCHAR(32)");
    columns.put("age", "VARCHAR(32)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");
    rm.createTable("read_table", schema);
    rm.createTable("write_table", schema);

    List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(ImmutableMap.of("id", 0, "first", "John", "last", "Doe", "age", 23));
    rows.add(ImmutableMap.of("id", 1, "first", "Jane", "last", "Doe", "age", 42));
    rows.add(ImmutableMap.of("id", 2, "first", "A", "last", "B", "age", 1));
    rm.write("read_table", rows);

    rm.cleanupAll();
  }
}
