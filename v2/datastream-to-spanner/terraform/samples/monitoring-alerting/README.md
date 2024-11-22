# Live Migration Monitoring Dashboard - Terraform Module

This Terraform module creates a Google Cloud Monitoring dashboard to visualize key metrics related to the Live migration template(s), including Cloud Storage, Pub/Sub, Dataflow, Spanner, and Datastream statistics. It includes log-based metrics and alert policies to monitor error thresholds, conversion errors, DLQ object counts, and throttling in various GCP services, ensuring a comprehensive view of the migration process.

## Overview
The Terraform configuration is organized into two main folders: main/ and modules/. The `main/` folder contains the core configuration files, including `main.tf`, where the infrastructure resources are defined, and module calls are made. `terraform_simple.tfvars`  and `terraform.tfvars` holds values for the variables defined in `variables.tf`, which are used throughout the configuration. The `provider.tf` file sets up the Google Cloud provider with necessary credentials and project details.

The `modules/` folder houses reusable Terraform modules. The `modules/dashboard` folder defines the Monitoring Dashboard resources, specifying how to visualize key metrics from various services used by the Live migration template(s) like Spanner, Dataflow, Pub/Sub, and Cloud Storage. The `modules/alerting` folder contains alert policies for various Google Cloud resources, with separate files for Google Cloud Storage, Pub/Sub, and Dataflow alerting. Finally, the `modules/notification_channels` folder configures the notification channels (email, SMS, etc.) that will be used to alert users when a condition is met.

## Requirements
* **Terraform:** Install Terraform on your local machine with version 0.13 or later
* **Google Cloud Provider:** Make sure you have the Google Cloud provider configured in your Terraform environment.
* **Google Cloud Project:** Create a Google Cloud project and enable the necessary APIs (e.g., Cloud Monitoring, Cloud Storage, Spanner). 
* **Service Account:** Create a service account with appropriate permissions to create Monitoring dashboards and access the relevant metrics.

## Usage
**1. Clone the Repository**
```
git clone <repository-url>
cd <repository-directory>
```
**2. Set Up Authentication**
* Set up your Google Cloud credentials using one of the following methods:
  * ***Application Default Credentials:*** Run `gcloud auth application-default login`. 
  * ***Service Account Key:*** Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your service account key file.

**3. Provide Variable Values**
* Update and mofify `input.tfvars` file in the main directory and provide the following values:
```hcl
  project_id = "<your-project-id>"
  prefix     = "<your-prefix>"
  email_address = "<notification-email-address>"
  # Optional:
  # region = "<your-desired-region>"
  # Customize thresholds as needed for alerting module
  gcs_object_count_dlq_threshold = 100
  gcs_read_write_throttles_threshold = 5000
  pubsub_age_of_oldest_message_threshold = 3600
  dataflow_conversion_errors_threshold = 10
  dataflow_other_errors_threshold = 50
  dataflow_total_errors_threshold = 100
```
**4. Update the `main.tf` to add more modules if necessary**
```hcl
    module "my_new_module" {
    source = "./modules/my_new_module"
    # ... (Provide any necessary variables for this module)
    }
```


**5. Initialize and Apply:**
```hcl
terraform init
terraform plan -var-file="terraform_simple.tfvars" -var-file="terraform.tfvars"
terraform apply -var-file="terraform_simple.tfvars" -var-file="terraform.tfvars"
```

**5. Clean and destroy:**
```hcl
terraform destroy -var-file="terraform_simple.tfvars" -var-file="terraform.tfvars"
```

