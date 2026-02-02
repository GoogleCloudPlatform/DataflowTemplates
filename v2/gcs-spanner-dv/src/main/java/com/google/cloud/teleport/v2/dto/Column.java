package com.google.cloud.teleport.v2.dto;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

@DefaultCoder(AvroCoder.class)
public class Column {

  private String colName;

  private String colValue;

  public Column() {
  }
}
