# UDF Samples

This directory contains sample Javascript UDFs that can be used to enrich, filter, route, and transform data in Dataflow pipelines.

## enrich.js

Adds a new field `source` with the value `pos` to the incoming JSON event.

## enrich_log.js

Adds fields to a Cloud Logging log entry received from Pub/Sub.  Adds `inputSubscription` and `callingAppId` based on the log entry content.

## filter.js

Filters out incoming JSON events where the `severity` field is equal to `DEBUG`.

## route.js

Routes incoming JSON events to the dead-letter queue if they do not have a `severity` property.  Throws an error with the event ID to trigger dead-letter queue routing.

## transform.js

Transforms fields of incoming JSON events. Normalizes the `source` field to lowercase, redacts `sensitiveField` to `REDACTED`, and removes `redundantField`.

## transform_csv.js

Transforms an incoming CSV line into a JSON object.  The output JSON object has fields `location`, `name`, `age`, `color`, and `coffee`, mapped from the corresponding CSV columns.

## transform_log.js

Transforms Cloud Logging log entries received from Pub/Sub.  If the log entry has a `textPayload`, it returns the `textPayload` as a string. Otherwise, it returns the original JSON object as a string.

## transform_log_splunk.js

Sets Splunk HTTP Event Collector (HEC) metadata for Cloud Logging log entries received from Pub/Sub.  Sets the `index`, `source`, and `sourcetype` metadata fields based on the log entry content.
