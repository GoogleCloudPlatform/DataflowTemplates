package com.google.cloud.teleport.v2.dofn;



import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessInformationSchemaFn extends DoFn<Void, Ddl> {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessInformationSchemaFn.class);

  private final SpannerConfig spannerConfig;

  private transient SpannerAccessor spannerAccessor;
  private transient Dialect dialect;

  public ProcessInformationSchemaFn(
      SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @Setup
  public void setup() throws Exception {
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);

    DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
    dialect =
        databaseAdminClient
            .getDatabase(spannerConfig.getInstanceId().get(), spannerConfig.getDatabaseId().get())
            .getDialect();
  }

  @Teardown
  public void teardown() throws Exception {
    if (spannerAccessor != null) {
      spannerAccessor.close();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Ddl mainDdl = getInformationSchemaAsDdl(spannerAccessor, dialect);
    c.output(mainDdl);
  }

  private Ddl getInformationSchemaAsDdl(SpannerAccessor accessor, Dialect dialect) {
    BatchClient batchClient = accessor.getBatchClient();
    BatchReadOnlyTransaction context =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
    return scanner.scan();
  }
}

