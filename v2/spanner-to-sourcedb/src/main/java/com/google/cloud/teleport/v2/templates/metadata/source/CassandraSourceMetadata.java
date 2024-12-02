package com.google.cloud.teleport.v2.templates.metadata.source;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.teleport.v2.spanner.migrations.shard.IShard;
import com.google.cloud.teleport.v2.templates.dbutils.connection.CassandraConnectionHelper;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.CassandraDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.schema.source.SourceColumn;
import com.google.cloud.teleport.v2.templates.schema.source.SourceSchema;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraSourceMetadata implements ISourceMetadata<SourceSchema> {

  private final String keyspace;
  private  final  List<IShard> shards;

  public CassandraSourceMetadata(List<IShard> shards) {
    this.keyspace = shards.get(0).getKeySpaceName();
    this.shards  = shards;
  }

  @Override
  public SourceSchema getMetadata() {
    Map<String, Map<String, SourceColumn>> schema = new HashMap<>();
    String query = "SELECT table_name, column_name, type, kind FROM system_schema.columns WHERE keyspace_name = '" + keyspace + "';";
    String connectionKey = shards.get(0).getHost() + ":" + shards.get(0).getPort() + "/" + shards.get(0).getUser() + "/" + shards.get(0).getKeySpaceName();
    try{
    IDao cassandraDao = new CassandraDao(connectionKey, shards.get(0).getUser(), new CassandraConnectionHelper());
    ResultSet resultSet = (ResultSet) cassandraDao.read(query);

      for (Row row : resultSet) {
        String tableName = row.getString("table_name");
        String columnName = row.getString("column_name");
        String dataType = row.getString("type");
        boolean isPrimaryKey = "partition_key".equals(row.getString("kind")) || "clustering".equals(row.getString("kind"));

        SourceColumn sourceColumn = new SourceColumn(dataType, isPrimaryKey);
        schema.computeIfAbsent(tableName, k -> new HashMap<>()).put(columnName, sourceColumn);
      }
    }catch (Exception e){
      throw new RuntimeException("Error while fetching metadata from Cassandra", e);
    }
    return new SourceSchema(schema);
  }
}