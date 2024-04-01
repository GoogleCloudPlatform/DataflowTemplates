package com.custom;

import com.google.cloud.teleport.v2.spanner.utils.GenericRequest;
import com.google.cloud.teleport.v2.spanner.utils.GenericResponse;
import com.google.cloud.teleport.v2.spanner.utils.IGenericRecordTransformer;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;

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
            GenericRecord sourceRecord = request.getRequestRow();
            Double a = (Double) sourceRecord.get("double_column");
            Integer b = (Integer) sourceRecord.get("int_column");
            Double c= a+b;
            int dateAsInt = (int) sourceRecord.get("date_column");
            LocalDate date = LocalDate.ofEpochDay(dateAsInt);
            LocalDate newDate = date.plusDays(1);
            int newDateAsInt = (int) newDate.toEpochDay();
            spannerRecord.put("sum", c);
            spannerRecord.put("date_column", newDateAsInt);
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
                    .name("sum").type().doubleType().noDefault() // New column in spanner that doesn't exist in source
                    .name("date_column").type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault() // Column exists in source but we are updating in spanner
                    .endRecord();
            return schema;
        }
        return null;
    }
}
