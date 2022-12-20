package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model;

import java.io.Serializable;

public class BigtableSource implements Serializable {
  private String instanceId;
  private String tableId;
  private String charset;

  private BigtableSource() {
  }

  public BigtableSource(String instanceId, String tableId, String charset) {
    this.instanceId = instanceId;
    this.tableId = tableId;
    this.charset = charset;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getTableId() {
    return tableId;
  }

  public String getCharset() {
    return charset;
  }
}
