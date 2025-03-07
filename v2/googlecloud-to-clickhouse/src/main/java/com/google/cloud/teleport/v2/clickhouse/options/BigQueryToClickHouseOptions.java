package com.google.cloud.teleport.v2.clickhouse.options;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;


public interface BigQueryToClickHouseOptions extends BigQueryConverters.BigQueryReadOptions, ClickHouseWriteOptions{
}
