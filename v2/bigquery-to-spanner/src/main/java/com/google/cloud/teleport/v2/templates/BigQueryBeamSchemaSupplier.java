package com.google.cloud.teleport.v2.templates;

import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.teleport.v2.templates.BigQueryUtils.BigQueryStorageClientFactory;
import com.google.cloud.teleport.v2.templates.BigQueryUtils.ReadSessionFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;

/**
 * Supplier for fetching Beam Schema from a BigQuery table, converted from it's Avro Schema.
 * This is needed when converting GenericRecord objects to Beam Row objects
 */
public class BigQueryBeamSchemaSupplier implements BeamSchemaSupplier {

  private String tableString;
  private TableReadOptions tableReadOptions;

  public BigQueryBeamSchemaSupplier(String tableString,
      TableReadOptions tableReadOptions) {
    this.tableString = tableString;
    this.tableReadOptions = tableReadOptions;
  }

  public org.apache.avro.Schema getAvroSchema() {
    org.apache.avro.Schema avroSchema;
    try (BigQueryStorageClient client = BigQueryStorageClientFactory.create()) {
      ReadSession session = ReadSessionFactory.create(client, tableString, tableReadOptions);
      // Extract schema from ReadSession
      avroSchema = new org.apache.avro.Schema.Parser()
          .parse(session.getAvroSchema().getSchema());
    }
    return avroSchema;
  }

  @Override
  public Schema get() {
    return AvroUtils.toBeamSchema(getAvroSchema());
  }
}
