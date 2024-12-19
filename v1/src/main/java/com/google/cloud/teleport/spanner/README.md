# Cloud Spanner Beam Pipelines

This directory contains source code for Apache Beam pipelines that interact with Cloud Spanner.

**Available Pipelines:**

* **Export:** Export data from Spanner to Avro files in Google Cloud Storage (`ExportPipeline.java`)
* **Import (Avro):** Import data from Avro files in Google Cloud Storage to Spanner (`ImportPipeline.java`)
* **Import (CSV):** Import data from CSV files in Google Cloud Storage to Spanner (`TextImportPipeline.java`)
