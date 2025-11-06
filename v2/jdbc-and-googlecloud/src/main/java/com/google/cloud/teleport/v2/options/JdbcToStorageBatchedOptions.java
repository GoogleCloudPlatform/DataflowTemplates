/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.options.JdbcToStorageOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Extra options for the Jdbc_to_Storage_Batched template.
 *
 * In addition to JdbcToStorageOptions (driverJars, driverClassName,
 * connectionUrl, username, password, KMSEncryptionKey, etc.), this template
 * accepts:
 *
 * - tableConfigs: JSON array of table configs
 * - maxParallelTables: advisory concurrency hint
 */
public interface JdbcToStorageBatchedOptions extends JdbcToStorageOptions {

    @Description("JSON array of table configs. Example:\n"
            + "[\n"
            + "  {\"table_name\":\"Pedido_PedidoStatus\",\n"
            + "   \"query\":\"SELECT * FROM Pedido_PedidoStatus WHERE DataInsercao >= '2025-01-01'\",\n"
            + "   \"output_directory\":\"gs://bucket/RAW/Pedido_PedidoStatus/...\",\n"
            + "   \"output_filename_prefix\":\"/part-\"}\n"
            + "]")
    @Validation.Required
    String getTableConfigs();

    void setTableConfigs(String value);

    @Description("Max number of tables to process in parallel (advisory only). "
            + "Used to size worker pool. Default 8.")
    String getMaxParallelTables();

    void setMaxParallelTables(String value);
}
