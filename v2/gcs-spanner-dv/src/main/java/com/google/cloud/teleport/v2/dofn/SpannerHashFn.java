package com.google.cloud.teleport.v2.dofn;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerHashFn extends DoFn<Struct, ComparisonRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerHashFn.class);

  private final PCollectionView<Ddl> ddlView;

  public SpannerHashFn(PCollectionView<Ddl> ddlView) {
    this.ddlView = ddlView;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Ddl ddl = c.sideInput(ddlView);
    String tableName = Objects.requireNonNull(c.element()).getString("__tableName__");
    Table table = ddl.table(tableName);
    List<String> primaryKeyColumns = table.primaryKeys().stream()
        .map(IndexColumn::name)
        .toList();

    ComparisonRecord comparisonRecord = ComparisonRecord.fromSpannerStruct(
        Objects.requireNonNull(c.element()), primaryKeyColumns);
    // LOG.info("spanner comparison record: {}", comparisonRecord.toString());
    c.output(comparisonRecord);
  }
}
