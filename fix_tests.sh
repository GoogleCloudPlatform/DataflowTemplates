#!/bin/bash
sed -i 's/sourceWriterFn.processElement(processContext, null);/sourceWriterFn.processElement(processContext, mockState);/g' v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/transforms/SourceWriterFnTest.java
sed -i '/public void setUp()/i \    @Mock private ValueState<String> mockState;' v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/transforms/SourceWriterFnTest.java
sed -i '/import org.apache.beam.sdk.transforms.DoFn.ProcessContext;/a \import org.apache.beam.sdk.state.ValueState;' v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/transforms/SourceWriterFnTest.java
