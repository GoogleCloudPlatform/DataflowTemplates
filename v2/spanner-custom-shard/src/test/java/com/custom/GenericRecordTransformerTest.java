package com.custom;

import com.google.cloud.teleport.v2.spanner.utils.GenericRequest;
import com.google.cloud.teleport.v2.spanner.utils.GenericResponse;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GenericRecordTransformerTest {
    @Test
    public void getSpannerRow() {
        Schema sourceSchema = SchemaBuilder.record("sample_table")
                .fields()
                .name("double_column").type().doubleType().noDefault()
                .name("int_column").type().intType().noDefault()
                .name("date_column").type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
                .endRecord();
        Schema spannerSchema = SchemaBuilder.record("sample_table")
                .fields()
                .name("sum").type().doubleType().noDefault()
                .name("date_column").type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
                .endRecord();
        GenericRecord sourceRecord = new GenericData.Record(sourceSchema);
        sourceRecord.put("double_column", 10.5);
        sourceRecord.put("int_column", 5);
        sourceRecord.put("date_column",19814); // Epoch date for 1 April 2024
        GenericRequest genericRequest = new GenericRequest("sample_table", sourceRecord, "", "");
        GenericRecordTransformer genericRecordTransformer = new GenericRecordTransformer();
        GenericResponse genericResponse = genericRecordTransformer.toSpannerRow(genericRequest);
        GenericRecord spannerRecord = new GenericData.Record(spannerSchema);
        spannerRecord.put("sum", 15.5);
        spannerRecord.put("date_column",19815); // Epoch date for 2 April 2024
        assertEquals(spannerRecord, genericResponse.getResponseRow());
    }
}
