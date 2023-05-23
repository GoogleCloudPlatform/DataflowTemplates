package com.google.cloud.syndeo.transforms.datagenerator;

import org.apache.avro.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.runner.RunWith;
import org.junit.Test;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class RecordCreatorTest {

  private static final String schemaString = "{\"type\":\"record\",\"name\":\"user_info_flat\",\"namespace\":\"com.google.syndeo\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"username\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"10\"},{\"name\":\"age\",\"type\":\"long\",\"default\":0},{\"name\":\"introduction\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"100\"},{\"name\":\"street\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"city\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"state\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"country\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"15\"}]}]}";

  @Test
  public void test() {
    Schema schema = Schema.parse(schemaString);
    Row row = RecordCreator.createRowRecord(schema);
    assertNotNull(row);
  }
}
