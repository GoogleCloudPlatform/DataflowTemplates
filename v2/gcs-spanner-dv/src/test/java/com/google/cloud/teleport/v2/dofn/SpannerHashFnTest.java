package com.google.cloud.teleport.v2.dofn;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import java.util.Collections;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class SpannerHashFnTest {

  @Test
  public void testProcessElement() {
    // Mock dependencies
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Struct, ComparisonRecord>.ProcessContext context = mock(DoFn.ProcessContext.class);
    SpannerHashFn doFn = new SpannerHashFn(ddlView);
    Ddl ddl = Ddl.builder().build();

    // Prepare input
    Struct inputStruct = Struct.newBuilder()
        .set("id").to(Value.int64(1L))
        .set("name").to(Value.string("test"))
        .build();

    // Define behavior
    when(context.sideInput(ddlView)).thenReturn(ddl);
    when(context.element()).thenReturn(inputStruct);

    // Execute
    doFn.processElement(context);

    // Verify output
    ArgumentCaptor<ComparisonRecord> argument = ArgumentCaptor.forClass(ComparisonRecord.class);
    verify(context).output(argument.capture());

    ComparisonRecord expected = ComparisonRecord.fromSpannerStruct(inputStruct,
        Collections.singletonList("id"));
    assertEquals(expected.getHash(), argument.getValue().getHash());
  }
}
