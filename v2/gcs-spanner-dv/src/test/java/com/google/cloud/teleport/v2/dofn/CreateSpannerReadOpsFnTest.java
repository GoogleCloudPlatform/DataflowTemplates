package com.google.cloud.teleport.v2.dofn;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class CreateSpannerReadOpsFnTest {

  @Test
  public void testProcessElement() {
    // Mock dependencies
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    // Prepare DDL behavior
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("Table1", "Table2"));

    // Define context behavior
    when(context.sideInput(ddlView)).thenReturn(ddl);

    // Create DoFn
    CreateSpannerReadOpsFn doFn = new CreateSpannerReadOpsFn(ddlView);

    // Execute
    doFn.processElement(context);

    // Verify output
    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(2)).output(argument.capture());

    // Validate captured arguments
    verify(context).output(ReadOperation.create().withQuery("SELECT * FROM Table1"));
    verify(context).output(ReadOperation.create().withQuery("SELECT * FROM Table2"));
  }
}
