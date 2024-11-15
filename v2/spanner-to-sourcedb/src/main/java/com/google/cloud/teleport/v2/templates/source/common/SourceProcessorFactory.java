package com.google.cloud.teleport.v2.templates.source.common;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.source.sql.SQLConnectionHelper;
import com.google.cloud.teleport.v2.templates.source.sql.SqlDao;
import com.google.cloud.teleport.v2.templates.source.sql.mysql.MySQLDMLGenerator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class SourceProcessorFactory {
  private static final Map<String, IDMLGenerator> DML_GENERATOR_MAP = Map.of(
          Constants.SOURCE_MYSQL, new MySQLDMLGenerator()
  );

  private static final Map<String, String> DRIVER_MAP = Map.of(
          Constants.SOURCE_MYSQL, "com.mysql.cj.jdbc.Driver"
  );

  private static final Map<String, Function<Shard, String>> CONNECTION_URL_GENERATORS = Map.of(
          Constants.SOURCE_MYSQL, shard ->
                  "jdbc:mysql://" + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName()
  );

  /**
   * Creates a SourceProcessor instance for the specified source type.
   *
   * @param source the type of the source database
   * @param shards the list of shards for the source
   * @param maxConnections the maximum number of connections
   * @return a configured SourceProcessor instance
   * @throws Exception if the source type is invalid
   */
  public static SourceProcessor createSourceProcessor(String source, List<Shard> shards, int maxConnections) throws Exception {
    IDMLGenerator dmlGenerator = getDMLGenerator(source);
    String driver = getDriver(source);
    SQLConnectionHelper connectionHelper = initializeConnectionHelper(source, shards, maxConnections, driver);
    Map<String, ISourceDao> sourceDaoMap = createSourceDaoMap(source, shards);

    return SourceProcessor.builder()
            .dmlGenerator(dmlGenerator)
            .sourceDaoMap(sourceDaoMap)
            .connectionHelper(connectionHelper)
            .build();
  }

  private static IDMLGenerator getDMLGenerator(String source) throws Exception {
    return Optional.ofNullable(DML_GENERATOR_MAP.get(source))
            .orElseThrow(() -> new Exception("Invalid source type for DML generator: " + source));
  }

  private static String getDriver(String source) throws Exception {
    return Optional.ofNullable(DRIVER_MAP.get(source))
            .orElseThrow(() -> new Exception("Invalid source type for driver: " + source));
  }

  private static SQLConnectionHelper initializeConnectionHelper(
          String source, List<Shard> shards, int maxConnections, String driver) throws Exception {
    SQLConnectionHelper connectionHelper = new SQLConnectionHelper();
    connectionHelper.init(shards, null, maxConnections, source, driver);
    return connectionHelper;
  }

  private static Map<String, ISourceDao> createSourceDaoMap(String source, List<Shard> shards) throws Exception {
    Function<Shard, String> urlGenerator = Optional.ofNullable(CONNECTION_URL_GENERATORS.get(source))
            .orElseThrow(() -> new Exception("Invalid source type for URL generation: " + source));

    Map<String, ISourceDao> sourceDaoMap = new HashMap<>();
    for (Shard shard : shards) {
      String connectionUrl = urlGenerator.apply(shard);
      ISourceDao sqlDao = new SqlDao(connectionUrl, shard.getUserName(), shard.getPassword());
      sourceDaoMap.put(shard.getLogicalShardId(), sqlDao);
    }
    return sourceDaoMap;
  }
}
