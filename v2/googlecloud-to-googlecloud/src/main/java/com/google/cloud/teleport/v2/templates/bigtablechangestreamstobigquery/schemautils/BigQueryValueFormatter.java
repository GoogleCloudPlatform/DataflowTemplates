package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import org.json.JSONObject;

public interface BigQueryValueFormatter {
  Object format(BigQueryUtils bigQuery, JSONObject chg) throws Exception;
}
