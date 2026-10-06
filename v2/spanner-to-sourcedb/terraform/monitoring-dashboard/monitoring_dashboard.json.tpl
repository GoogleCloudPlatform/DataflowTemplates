{
  "displayName": "${dashboard_display_name}",
  "dashboardFilters": [
    {
      "filterType": "VALUE_ONLY",
      "templateVariable": "sla_seconds",
      "valueType": "STRING",
      "stringValue": "300"
    }
  ],
  "mosaicLayout": {
    "columns": 12,
    "tiles": [
      {
        "yPos": 0,
        "xPos": 0,
        "width": 12,
        "height": 4,
        "widget": {
          "title": "Cutback Readiness Status (1 = READY, 0 = NOT READY)",
          "scorecard": {
            "thresholds": [
              {
                "color": "RED",
                "direction": "BELOW",
                "label": "NOT READY",
                "value": 1
              }
            ],
            "timeSeriesQuery": {
              "prometheusQuery": "(((max(custom_googleapis_com:migration_gcs_dlq_file_count{monitored_resource=\"global\",migration_id=\"${migration_id}\",dlq_category=\"severe\"}) or vector(0)) == bool 0) * ((max(avg_over_time(dataflow_googleapis_com:job_data_watermark_age{monitored_resource=\"dataflow_job\",job_id=~\"${dataflow_job_ids}\"}[1m])) or vector(0)) <= bool $${sla_seconds}) * ((max(avg_over_time(dataflow_googleapis_com:job_system_lag{monitored_resource=\"dataflow_job\",job_id=~\"${dataflow_job_ids}\"}[1m])) or vector(0)) <= bool $${sla_seconds}) * ((max(sum by (subscription_id) (avg_over_time(pubsub_googleapis_com:subscription_num_unacked_messages_by_region{monitored_resource=\"pubsub_subscription\",subscription_id=~\"${pubsub_subscription_ids}\"}[1m]))) or vector(0)) <= bool 50) * ((max(deriv((sum by (subscription_id) (avg_over_time(pubsub_googleapis_com:subscription_num_unacked_messages_by_region{monitored_resource=\"pubsub_subscription\",subscription_id=~\"${pubsub_subscription_ids}\"}[1m])))[5m:1m])) or vector(0)) <= bool 5))",
              "unitOverride": ""
            }
          }
        }
      },
      {
        "yPos": 4,
        "xPos": 0,
        "width": 12,
        "height": 6,
        "widget": {
          "title": "GCS Dead-Letter Queue (DLQ)",
          "collapsibleGroup": {
            "collapsed": false
          }
        }
      },
      {
        "yPos": 6,
        "xPos": 0,
        "width": 12,
        "height": 4,
        "widget": {
          "title": "GCS Dead-Letter Queue (DLQ) Events Over Time",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "metric.type=\"custom.googleapis.com/migration/gcs_dlq_file_count\" resource.type=\"global\" metric.label.\"migration_id\"=\"${migration_id}\"",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_MAX",
                      "crossSeriesReducer": "REDUCE_MAX",
                      "groupByFields": [
                        "metric.label.\"dlq_category\""
                      ]
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "yPos": 10,
        "xPos": 0,
        "width": 12,
        "height": 6,
        "widget": {
          "title": "Dataflow",
          "collapsibleGroup": {
            "collapsed": false
          }
        }
      },
      {
        "yPos": 12,
        "xPos": 0,
        "width": 4,
        "height": 4,
        "widget": {
          "title": "Data Freshness",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "metric.type=\"dataflow.googleapis.com/job/data_watermark_age\" resource.type=\"dataflow_job\" metric.label.\"job_id\"=monitoring.regex.full_match(\"${dataflow_job_ids}\")",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_MEAN",
                      "crossSeriesReducer": "REDUCE_MAX",
                      "groupByFields": [
                        "metric.label.\"job_id\""
                      ]
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "yPos": 12,
        "xPos": 4,
        "width": 4,
        "height": 4,
        "widget": {
          "title": "System Latency",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "metric.type=\"dataflow.googleapis.com/job/system_lag\" resource.type=\"dataflow_job\" metric.label.\"job_id\"=monitoring.regex.full_match(\"${dataflow_job_ids}\")",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_MEAN",
                      "crossSeriesReducer": "REDUCE_MAX",
                      "groupByFields": [
                        "metric.label.\"job_id\""
                      ]
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "yPos": 12,
        "xPos": 8,
        "width": 4,
        "height": 4,
        "widget": {
          "title": "Source DB Write Throughput",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" metric.label.\"job_id\"=monitoring.regex.full_match(\"${dataflow_job_ids}\") metric.label.\"ptransform\"=\"Write to source/Write to sourcedb\"",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_RATE",
                      "crossSeriesReducer": "REDUCE_SUM",
                      "groupByFields": [
                        "metric.label.\"job_id\""
                      ]
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "yPos": 16,
        "xPos": 0,
        "width": 12,
        "height": 6,
        "widget": {
          "title": "Pub/Sub",
          "collapsibleGroup": {
            "collapsed": false
          }
        }
      },
      {
        "yPos": 18,
        "xPos": 0,
        "width": 12,
        "height": 4,
        "widget": {
          "title": "Unacknowledged Message Count",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "metric.type=\"pubsub.googleapis.com/subscription/num_unacked_messages_by_region\" resource.type=\"pubsub_subscription\" resource.label.\"subscription_id\"=monitoring.regex.full_match(\"${pubsub_subscription_ids}\")",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_MEAN",
                      "crossSeriesReducer": "REDUCE_SUM",
                      "groupByFields": [
                        "resource.label.\"subscription_id\""
                      ]
                    }
                  }
                }
              }
            ]
          }
        }
      }
    ]
  }
}
