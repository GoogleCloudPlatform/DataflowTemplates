package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.JdbcToStorageOptions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link JdbcExecutor} pipeline executes arbitrary SQL or PL/SQL statements on a JDBC source.
 * It is similar to JdbcToStorage, but uses Statement.execute() instead of executeQuery(),
 * and does not produce any output to GCS.
 *
 * Typical use case: DDL/DML/PLSQL blocks for Oracle, e.g. BEGIN ... END;.
 */
@Template(
    name = "Jdbc_Executor",
    category = TemplateCategory.BATCH,
    displayName = "JDBC Executor (no output)",
    description = "Executes SQL or PL/SQL statements on a JDBC source (e.g. Oracle BEGIN...END; blocks). "
        + "No output is written to Cloud Storage.",
    optionsClass = JdbcToStorageOptions.class,
    flexContainerName = "jdbc-executor",
    documentation = "https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-executor",
    contactInformation = "https://cloud.google.com/support",
    preview = true
)
public class JdbcExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcExecutor.class);

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();
    JdbcToStorageOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JdbcToStorageOptions.class);
    run(options);
  }

  public static PipelineResult run(JdbcToStorageOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    LOG.info("Starting Jdbc-Executor pipeline.");

    String driverClass = options.getDriverClassName();
    String url = maybeDecrypt(options.getConnectionUrl(), options.getKMSEncryptionKey());
    String username = maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey());
    String password = maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey());
    String sql = options.getQuery();

    LOG.info("Connecting to {} using driver {}", url, driverClass);
    LOG.info("Executing SQL:\n{}", sql);

    pipeline.apply("ExecuteSQL", org.apache.beam.sdk.transforms.Create.of(1))
        .apply("RunStatement", org.apache.beam.sdk.transforms.ParDo.of(
            new org.apache.beam.sdk.transforms.DoFn<Integer, Void>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                try {
                  Class.forName(driverClass);
                  try (Connection conn = DriverManager.getConnection(url, username, password);
                       Statement stmt = conn.createStatement()) {
                    boolean result = stmt.execute(sql);
                    conn.commit();
                    LOG.info("Statement executed successfully. Result = {}", result);
                  }
                } catch (Exception e) {
                  LOG.error("Error executing SQL statement", e);
                  throw new RuntimeException(e);
                }
              }
            }
        ));

    return pipeline.run();
  }
}


