package com.google.cloud.teleport.v2.source.oracle.reader.io.jdbc.iowrapper.config.defaults;

import com.google.cloud.teleport.v2.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.oracle.reader.io.jdbc.dialectadapter.oracle.OracleDialectAdapter;
import com.google.cloud.teleport.v2.source.oracle.reader.io.jdbc.rowmapper.provider.OracleJdbcValueMappings;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;

public class OracleConfigDefaults {
  public static final DialectAdapter DEFAULT_ORACLE_DIALECT_ADAPTER = new OracleDialectAdapter();
  public static final JdbcValueMappingsProvider DEFAULT_ORACLE_VALUE_MAPPING_PROVIDER = new OracleJdbcValueMappings();
  public static final Long DEFAULT_ORACLE_MAX_CONNECTIONS = 160L;
  public static final FluentBackoff DEFAULT_ORACLE_SCHEMA_DISCOVERY_BACKOFF = FluentBackoff.DEFAULT.withMaxCumulativeBackoff(Duration.standardMinutes(5L));
  public static final ImmutableList<String> DEFAULT_ORACLE_INIT_SEQ = ImmutableList.of();
  private OracleConfigDefaults() {}
}
