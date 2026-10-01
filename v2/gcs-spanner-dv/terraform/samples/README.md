# Terraform Samples for GCS to Spanner Data Validation

This directory provides samples for common scenarios users might have while trying to run a data validation job.

Pick a sample that is closest to your use-case, and use it as a starting point, tailoring it to your own specific needs.

## Prerequisites

Before using these samples, ensure you have:
- [Terraform](https://developer.hashicorp.com/terraform/downloads) installed.
- Authenticated with Google Cloud (e.g., using `gcloud auth application-default login`).
- A Google Cloud project with the necessary permissions.

## List of examples

1. [Launching a single Data Validation Job](simple-validation-job/README.md) - A basic example that configures the necessary variables and provisions the Dataflow pipeline for GCS to Spanner Data Validation.

## How to add a new sample

We strongly recommend copying an existing sample and modifying it for your scenario. This ensures a consistent style across all Terraform samples.

```shell
mkdir my-new-sample
cp -r simple-validation-job/ my-new-sample/
cd my-new-sample/
```
