package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableCreator;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beam transform which 1) Reads information schema from both main and shadow table database. 2)
 * Create shadow tables in the shadow table database 3) Outputs both DDL schemas
 */
public class SpannerToSourceDbProcessInformationSchema extends PTransform<PBegin, PCollectionTuple> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbProcessInformationSchema.class);

  public static final TupleTag<Ddl> MAIN_DDL_TAG = new TupleTag<>() {};
  public static final TupleTag<Ddl> SHADOW_TABLE_DDL_TAG = new TupleTag<>() {};

  private final SpannerConfig spannerConfig;
  private final SpannerConfig shadowTableSpannerConfig;
  private final String shadowTablePrefix;

  public SpannerToSourceDbProcessInformationSchema(
      SpannerConfig spannerConfig,
      SpannerConfig shadowTableSpannerConfig,
      String shadowTablePrefix) {
    this.spannerConfig = spannerConfig;
    this.shadowTableSpannerConfig = shadowTableSpannerConfig;
    this.shadowTablePrefix = shadowTablePrefix;
  }

  @Override
  public PCollectionTuple expand(PBegin p) {
    return p.apply("Create empty", Create.of((Void) null))
        .apply(
            "Create Shadow tables and return Information Schemas",
            ParDo.of(
                    new ProcessInformationSchemaFn(
                        spannerConfig,
                        shadowTableSpannerConfig,
                        shadowTablePrefix))
                .withOutputTags(MAIN_DDL_TAG, TupleTagList.of(SHADOW_TABLE_DDL_TAG)));
  }

  static class ProcessInformationSchemaFn extends DoFn<Void, Ddl> {
    private final SpannerConfig spannerConfig;
    private final SpannerConfig shadowTableSpannerConfig;
    private final String shadowTablePrefix;

    private transient SpannerAccessor spannerAccessor;
    private transient SpannerAccessor shadowTableSpannerAccessor;
    private transient Dialect dialect;
    private transient Dialect shadowTableDialect;

    public ProcessInformationSchemaFn(
        SpannerConfig spannerConfig,
        SpannerConfig shadowTableSpannerConfig,
        String shadowTablePrefix) {
      this.spannerConfig = spannerConfig;
      this.shadowTableSpannerConfig = shadowTableSpannerConfig;
      this.shadowTablePrefix = shadowTablePrefix;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
      shadowTableSpannerAccessor = SpannerAccessor.getOrCreate(shadowTableSpannerConfig);

      DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
      dialect =
          databaseAdminClient
              .getDatabase(spannerConfig.getInstanceId().get(), spannerConfig.getDatabaseId().get())
              .getDialect();

      DatabaseAdminClient shadowDatabaseAdminClient = shadowTableSpannerAccessor.getDatabaseAdminClient();
      shadowTableDialect =
          shadowDatabaseAdminClient
              .getDatabase(shadowTableSpannerConfig.getInstanceId().get(), shadowTableSpannerConfig.getDatabaseId().get())
              .getDialect();
    }

    @Teardown
    public void teardown() throws Exception {
      if (spannerAccessor != null) {
        spannerAccessor.close();
      }
      if (shadowTableSpannerAccessor != null) {
        shadowTableSpannerAccessor.close();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      LOG.info("Starting processing of Information Schema...");
      
      // 1. Create Shadow Tables if needed
      ShadowTableCreator shadowTableCreator =
          new ShadowTableCreator(
              spannerConfig,
              shadowTableSpannerConfig,
              shadowTableDialect,
              shadowTablePrefix);
      
      // This method internally fetches DDLs and creates tables. 
      // We might want to optimize this in the future, but for now moving it to worker is the goal.
      shadowTableCreator.createShadowTablesInSpanner();

      // 2. Fetch DDLs
      Ddl mainDdl = getInformationSchemaAsDdl(spannerAccessor, dialect);
      Ddl shadowTableDdl = getInformationSchemaAsDdl(shadowTableSpannerAccessor, shadowTableDialect);

      c.output(MAIN_DDL_TAG, mainDdl);
      c.output(SHADOW_TABLE_DDL_TAG, shadowTableDdl);
      LOG.info("Finished processing of Information Schema.");
    }

    private Ddl getInformationSchemaAsDdl(SpannerAccessor accessor, Dialect dialect) {
      BatchClient batchClient = accessor.getBatchClient();
      BatchReadOnlyTransaction context =
          batchClient.batchReadOnlyTransaction(TimestampBound.strong());
      InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
      return scanner.scan();
    }
  }
}
