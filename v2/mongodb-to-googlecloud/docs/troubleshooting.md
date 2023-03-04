# Troubleshooting MongoDB Template Issues

## Symptom: Unable to connect to MongoDB

1. Check the authentication settings on the MongoDB side and if the user has read and write access to any database in the project. Refer [documentation](https://www.mongodb.com/docs/atlas/security-add-mongodb-users/) for more details.
2. Check the IP address allowlist on MongoDB Atlas. If you are using the default network on GCP VPC, allowlist only the IP address for the specific region you are running the template in. Refer [documentation](https://www.mongodb.com/docs/atlas/security/ip-access-list/) for more details.

This could also happen due to one of several internal or external issues including misconfigured MongoDB endpoints. Follow these steps to identify root cause:

1. Open GCP Metrics Explorer.
2. Pick Resource type:“Dataflow Job” and Metrics:“outbound-successful-events”.
3. Confirm that counter **has not increased** since X period of time.
   * Otherwise, this is MongoDB-related issue e.g. user access control change.
4. Open GCP Dataflow Job Graph and set time range to X period of time.
5. Select Pipeline Step “FlattenErrors”.
6. Inside the panel, inspect the number of elements added in each input collection.
7. Open Cloud Logging Logs Viewer, set time range to X period of time, and type following query, replacing DATAFLOW_JOB_ID:

   Example error due to incorrect MongoDB SSL configuration:
   * Error message: "**Error writing to MongoDB: sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target**".
     * This can be checked by running `openssl s_client -showcerts -servername youtube.com -connect www.youtube.com:443 2>/dev/null </dev/null| openssl x509 -text`, with youtube.com replaced with your MongoDB hostname. The result should contain correct Issuer and Subject like follows (for youtube.com):
     ```
     Issuer: C = US, O = Google Trust Services, CN = GTS CA 1O1
     Subject: C = US, ST = California, L = Mountain View, O = Google LLC, CN = *.google.com
     ```
   * Resolution: use valid CA-signed SSL certificates for the MongoDB  endpoint. Alternatively, you can re-deploy pipeline with parameter disableCertificateValidation=true (recommended only for test/dev workload).
   * For more details on Self-managed X.509 Authentication refer [here](https://www.mongodb.com/docs/atlas/security-self-managed-x509/#set-up-self-managed-x-509-authentication).
8. If no errors found, inspect error attribute in undelivered messages that are written to Pub/Sub dead letter topic:
   * Open Pub/Sub Subscriptions and select dead letter subscription used.
   * Click on ‘View Messages’ and then ‘Pull’ (keep ‘Enable ack messages’ unchecked).
   * Inspect attribute.errorMessage column.

## Symptom: Handshake failure while writing to MongoDB

**Example error message:**

```
Error writing to MongoDB: Received fatal alert: handshake_failure
```

**Possible causes:**

1. SSL certificate may have expired midway during the job. Setting `disableCertificateValidation=true` as part of pipeline options would resolve the issue in that case. The above step is generally not recommended for production environments and should be used only for testing purposes to identify the root cause.

2. There might have been a change on the MongoDB endpoint. In that case, ask MongoDB (file a support ticket) to revert back their changes for the endpoint to the previously working ciphersuites versions or MongoDB to add Dataflow-supported ciphersuites.

## Symptom: "Failed to start reading from source" error

1. Check if you have the right authentication for passing with your MongoDB URI. 
2. Login into your Atlas cluster and navigate to the Network tab. Check if you have allowlisted the IP address you are trying to connect from (this would be the IP address of the Dataflow worker nodes, which run on the default VPC if not specified by the user in optional parameters).

## Symptom: "Cannot read and write in different locations" error

Dataflow templates should use the same region as that of the BigQuery dataset. Check if the region of the Dataflow template selected is the same as that of the BigQuery dataset you have created.

## Symptom: "Provided Schema does not match Table" error when writing to BigQuery

Check if the table you are referring to for BigQuery already exists. If it does, it should match the fields of the MongoDB keys of the collection you are referring to while building the pipeline and also check if the document is loaded as is using `userOption` `NONE` or `FLATTEN` (if the user option is `NONE`, there will be only 2 columns and one of them will be `source_data` and other will be `Timestamp`).

## Symptom: MongoDB quota exceeded

The threshold values depend on the cluster type you are using. All the limits are published [here](https://www.mongodb.com/docs/atlas/reference/atlas-limits/).