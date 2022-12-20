package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model;

public enum ModType {
  SET_CELL("SET_CELL"),
  DELETE_CELLS("DELETE_CELLS"),
  DELETE_FAMILY("DELETE_FAMILY"),
  UNKNOWN("UNKNOWN");

  private final String code;

  ModType(String code) {
    this.code = code;
  }

  public String getCode() {
    return code;
  }
}
