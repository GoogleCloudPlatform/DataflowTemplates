package com.custom;

import com.google.cloud.teleport.v2.spanner.utils.GenericRequest;
import com.google.cloud.teleport.v2.spanner.utils.GenericResponse;
import com.google.cloud.teleport.v2.spanner.utils.IGenericRecordTransformer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericRecordTransformer implements IGenericRecordTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(GenericRecordTransformer.class);
    @Override
    public void init(String customParameters) {
        LOG.info("init called with {}", customParameters);
    }

    @Override
    public GenericResponse toSpannerRow(GenericRequest request) {

        GenericRecord spannerRecord = new GenericData.Record(getSchema(request.getTableName()));
        if (request.getTableName().equals("sample_table")) {
            Double a = (Double) request.getRequestRow().get("double_column");
            Integer b = (Integer) request.getRequestRow().get("int_column");
            Double c= a+b;
            spannerRecord.put("sum", c);
        }
        return new GenericResponse(spannerRecord, false);
    }

    @Override
    public GenericResponse toSourceRow(GenericRequest request) {
        return null;
    }

    private Schema getSchema(String tableName) {

        if(tableName.equals("sample_table")) {
            Schema schema = SchemaBuilder.record(tableName)
                    .fields()
                    .name("sum").type().doubleType().noDefault()
                    .endRecord();
            return schema;
        }
        return null;
    }
}
