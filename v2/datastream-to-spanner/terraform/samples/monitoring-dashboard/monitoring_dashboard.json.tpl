{
  "displayName": "${dashboard_display_name}",
  "mosaicLayout": {
    "columns": 12,
    "tiles": [
      {
        "yPos": 0,
        "xPos": 0,
        "width": 12,
        "height": 6,
        "widget": {
          "title": "Datastream",
          "collapsibleGroup": {
            "collapsed": false
          }
        }
      },
      {
        "yPos": 2,
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
                    "filter": "metric.type=\"datastream.googleapis.com/stream/freshness\" resource.type=\"datastream.googleapis.com/Stream\" resource.label.\"stream_id\"=monitoring.regex.full_match(\"${datastream_ids}\")",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_MIN",
                      "crossSeriesReducer": "REDUCE_MAX"
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "yPos": 2,
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
                    "filter": "metric.type=\"datastream.googleapis.com/stream/system_latencies\" resource.type=\"datastream.googleapis.com/Stream\" resource.label.\"stream_id\"=monitoring.regex.full_match(\"${datastream_ids}\")",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_DELTA",
                      "crossSeriesReducer": "REDUCE_PERCENTILE_99"
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "yPos": 2,
        "xPos": 8,
        "width": 4,
        "height": 4,
        "widget": {
          "title": "Throughput (events/sec)",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "metric.type=\"datastream.googleapis.com/stream/event_count\" resource.type=\"datastream.googleapis.com/Stream\" resource.label.\"stream_id\"=monitoring.regex.full_match(\"${datastream_ids}\")",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_RATE",
                      "crossSeriesReducer": "REDUCE_SUM"
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "yPos": 6,
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
        "yPos": 8,
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
                      "crossSeriesReducer": "REDUCE_MAX"
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "yPos": 8,
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
                      "crossSeriesReducer": "REDUCE_MAX"
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "yPos": 8,
        "xPos": 8,
        "width": 4,
        "height": 4,
        "widget": {
          "title": "Spanner Write Throughput",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "metric.type=\"dataflow.googleapis.com/job/elements_produced_count\" resource.type=\"dataflow_job\" metric.label.\"job_id\"=monitoring.regex.full_match(\"${dataflow_job_ids}\") metric.label.\"ptransform\"=\"Write events to Cloud Spanner/Write Mutations\"",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_RATE",
                      "crossSeriesReducer": "REDUCE_SUM"
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
        "yPos": 14,
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
                      "crossSeriesReducer": "REDUCE_SUM"
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