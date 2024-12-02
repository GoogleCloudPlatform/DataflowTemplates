package com.google.cloud.teleport.v2.templates.metadata.source;

import com.google.cloud.teleport.v2.spanner.migrations.shard.IShard;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.schema.source.SourceSchema;

import java.util.List;

public class SourceMetadataFactory {

  public static ISourceMetadata<SourceSchema> getSourceMetadata(String sourceType, List<IShard> iShards) {
    switch (sourceType) {
      case "Cassandra":
        return new CassandraSourceMetadata(iShards);
      case "Mysql":
        return  null;
      // Add cases for other sourceDbTypes here
      default:
        throw new IllegalArgumentException("Unsupported sourceDbType: " + sourceType);
    }
  }
}