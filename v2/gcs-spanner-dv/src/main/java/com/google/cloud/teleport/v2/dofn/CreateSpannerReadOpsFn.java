package com.google.cloud.teleport.v2.dofn;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

public class CreateSpannerReadOpsFn extends DoFn<Void, ReadOperation> {

  private final PCollectionView<Ddl> ddlView;

  public CreateSpannerReadOpsFn(PCollectionView<Ddl> ddlView) {
    this.ddlView = ddlView;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Ddl ddl = c.sideInput(ddlView);
    List<String> tableNames = ddl.getTablesOrderedByReference();
    tableNames.forEach(tableName -> {
      String query = String.format("SELECT *, '%s' as __tableName__ FROM %s", tableName, tableName);
      c.output(ReadOperation.create().withQuery(query));
    });
  }
}
