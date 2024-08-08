/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.bigtable;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.cloud.teleport.util.GCSAwareValueProvider;
import com.google.cloud.teleport.util.ValueProcessor;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class processes a Cassandra column schema string into a writetime query string. */
class CassandraColumnSchemaProcessor implements ValueProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraColumnSchemaProcessor.class);

  @Override
  public String process(String fileString) {
    LOG.info("Cassandra column schema provided, parsing file string.");
    // Parse Cassandra column schema to generate writetime-enriched CassandraIO query.
    CassandraColumnSchema schema = new CassandraColumnSchema(fileString);

    // Add writetime query to CassandraIO.
    String writeTimeQuery = schema.createWritetimeQuery();
    LOG.info("Write time query generated " + writeTimeQuery);
    return writeTimeQuery;
  }
}

/**
 * Value provider that explicitly looks for a Cassandra column schema file and provides either a
 * selectAll query or selectAll+writetime query.
 */
public class CassandraWritetimeQueryProvider extends GCSAwareValueProvider {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraWritetimeQueryProvider.class);
  private final ValueProvider<String> cassandraKeyspace;
  private final ValueProvider<String> cassandraTable;

  public CassandraWritetimeQueryProvider(
      ValueProvider<String> cassandraColumnSchema,
      ValueProvider<String> cassandraKeyspace,
      ValueProvider<String> cassandraTable) {
    super(cassandraColumnSchema, new CassandraColumnSchemaProcessor());
    this.cassandraKeyspace = cassandraKeyspace;
    this.cassandraTable = cassandraTable;
  }

  /**
   * Provides either a selectAll statement or selectAll+writeTime statement. This is necessary in
   * order for CassandraIO to query dynamically.
   *
   * @return String CQL query string.
   */
  @Override
  public synchronized String get() {
    if (super.get() == null) {
      // Construct select-all statement in case column schema is not provided.
      Select selectAll = selectFrom(cassandraKeyspace.get(), cassandraTable.get()).all();

      LOG.info("No column schema provided, returning selectAll query " + selectAll.asCql());
      return selectAll.asCql();
    }
    return super.get();
  }
}
