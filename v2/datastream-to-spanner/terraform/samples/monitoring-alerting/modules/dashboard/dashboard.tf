locals {
  dashboard_json = jsonencode({
    "displayName": "${var.prefix} - Live Migration Monitoring Dashboard",
    "gridLayout": {
      "columns": "2",
      "widgets": [
        {
          "text": {
            "content": "# View Spanner and Dataflow Dashboards\n\n\n\n\n\n\n\n##### **GCP Spanner Dashboard:**  \n[Open Spanner Dashboard](https://console.cloud.google.com/monitoring/dashboards/resourceList/spanner_instance?project=${var.spanner_project_id})  \n\n##### **GCP Dataflow Dashboard:**  \n[Open Dataflow Dashboard](https://console.cloud.google.com/monitoring/dashboards/resourceList/dataflow_job;filters=type:search,val:${var.prefix}?project=${var.project_id})"
          }
        },
        {
          "title": "Cloud Storage - Request Count",
          "xyChart": {
            "chartOptions": {
              "mode": "COLOR"
            },
            "dataSets": [
              {
                "minAlignmentPeriod": "60s",
                "plotType": "STACKED_BAR",
                "targetAxis": "Y1",
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_RATE"
                    },
                    "filter": "metric.type=\"storage.googleapis.com/api/request_count\" AND resource.type=\"gcs_bucket\" AND resource.labels.project_id=${var.project_id} AND resource.labels.bucket_name=starts_with(${var.prefix})"
                  }
                }
              }
            ],
            "timeshiftDuration": "0s",
            "yAxis": {
              "label": "requests/s",
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Cloud Storage - Network Sent Bytes",
          "xyChart": {
            "chartOptions": {
              "mode": "COLOR"
            },
            "dataSets": [
              {
                "minAlignmentPeriod": "60s",
                "plotType": "LINE",
                "targetAxis": "Y1",
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_RATE"

                    },
                    "filter": "metric.type=\"storage.googleapis.com/network/sent_bytes_count\" AND resource.type=\"gcs_bucket\" AND resource.labels.project_id=${var.project_id} AND resource.labels.bucket_name=starts_with(${var.prefix})"
                  }
                }
              }
            ],
            "timeshiftDuration": "0s",
            "yAxis": {
              "label": "bytes/s",
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Cloud Storage - Network Received Bytes",
          "xyChart": {
            "chartOptions": {
              "mode": "COLOR"
            },
            "dataSets": [
              {
                "minAlignmentPeriod": "60s",
                "plotType": "LINE",
                "targetAxis": "Y1",
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_RATE"

                    },
                    "filter": "metric.type=\"storage.googleapis.com/network/received_bytes_count\" AND resource.type=\"gcs_bucket\" AND resource.labels.project_id=${var.project_id} AND resource.labels.bucket_name=starts_with(${var.prefix})"
                  }
                }
              }
            ],
            "timeshiftDuration": "0s",
            "yAxis": {
              "label": "bytes/s",
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Cloud Storage - Object Count",
          "xyChart": {
            "chartOptions": {
              "mode": "COLOR"
            },
            "dataSets": [
              {
                "minAlignmentPeriod": "60s",
                "plotType": "LINE",
                "targetAxis": "Y1",
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "crossSeriesReducer": "REDUCE_SUM",

                      "perSeriesAligner": "ALIGN_MEAN"
                    },
                    "filter": "metric.type=\"storage.googleapis.com/storage/object_count\" AND resource.type=\"gcs_bucket\" AND resource.labels.project_id=${var.project_id} AND resource.labels.bucket_name=starts_with(${var.prefix})"

                  }
                }
              }
            ],
            "timeshiftDuration": "0s",
            "yAxis": {
              "label": "count",
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Cloud Storage - Total Storage Bytes",
          "xyChart": {
            "chartOptions": {
              "mode": "COLOR"
            },
            "dataSets": [
              {
                "minAlignmentPeriod": "60s",
                "plotType": "LINE",
                "targetAxis": "Y1",
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "crossSeriesReducer": "REDUCE_SUM",

                      "perSeriesAligner": "ALIGN_MEAN"
                    },
                    "filter": "metric.type=\"storage.googleapis.com/storage/total_bytes\" AND resource.type=\"gcs_bucket\" AND resource.labels.project_id=${var.project_id} AND resource.labels.bucket_name=starts_with(${var.prefix})"

                  }
                }
              }
            ],
            "timeshiftDuration": "0s",
            "yAxis": {
              "label": "bytes",
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Pub/Sub - Publish Message Count",
          "xyChart": {
            "chartOptions": {
              "mode": "COLOR"
            },
            "dataSets": [
              {
                "minAlignmentPeriod": "60s",
                "plotType": "LINE",
                "targetAxis": "Y1",
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_RATE"

                    },
                    "filter": "metric.type=\"pubsub.googleapis.com/topic/send_request_count\" AND resource.type=\"pubsub_topic\" AND resource.labels.project_id=${var.project_id} AND resource.labels.topic_id=starts_with(${var.prefix})"

                  }
                }
              }
            ],
            "timeshiftDuration": "0s",
            "yAxis": {
              "label": "messages/s",
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Pub/Sub - Average Message Size",
          "xyChart": {
            "chartOptions": {
              "mode": "COLOR"
            },
            "dataSets": [
              {
                "plotType": "STACKED_BAR",
                "targetAxis": "Y1",
                "timeSeriesQuery": {
                  "timeSeriesQueryLanguage": "fetch pubsub.googleapis.com/topic/message_sizes | filter (resource.project_id == \"${var.project_id}\") | filter (resource.topic_id =~ \"^${var.prefix}.*\") | every 60s | group_by [resource.topic_id], [value_message_sizes: mean(value.message_sizes)]"
                }
              }
            ],
            "timeshiftDuration": "0s",
            "yAxis": {
              "label": "bytes",
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Pub/Sub - Subscription Backlog",
          "xyChart": {
            "chartOptions": {
              "mode": "COLOR"
            },
            "dataSets": [
              {
                "plotType": "LINE",
                "targetAxis": "Y1",
                "timeSeriesQuery": {
                  "timeSeriesQueryLanguage": "fetch pubsub_subscription\n| metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age'\n| filter resource.project_id == '${var.prefix}'\n  && metadata.system_labels.topic_id =~ '^${var.prefix}.*'\n| group_by 1m,\n  [value_oldest_unacked_message_age_mean:\n max(value.oldest_unacked_message_age)]\n| every 1m\n| top 5, .max()"
                }
              }
            ],
            "timeshiftDuration": "0s",
            "yAxis": {
              "label": "Age of Oldest Unacked Message (seconds)",
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Datastream: System Latency",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesQueryLanguage": "fetch datastream.googleapis.com/Stream| metric \"datastream.googleapis.com/stream/system_latencies\"| filter (resource.stream_id =~ \"^${var.prefix}.*\") | filter (resource.resource_container == \"${var.project_id}\") | align delta(1m)| every 1m| group_by [resource.stream_id], [value_system_latencies_percentile: percentile(value.system_latencies,50)]"
                },
                "plotType": "LINE",
                "targetAxis": "Y1"
              },
              {
                "timeSeriesQuery": {
                  "timeSeriesQueryLanguage": "fetch datastream.googleapis.com/Stream| metric \"datastream.googleapis.com/stream/system_latencies\"| filter (resource.stream_id =~ \"^${var.prefix}.*\") | filter (resource.resource_container == \"${var.project_id}\")| align delta(1m)| every 1m| group_by [resource.stream_id], [value_system_latencies_percentile: percentile(value.system_latencies,90)]"
                },
                "plotType": "LINE",
                "targetAxis": "Y1"
              },
              {
                "timeSeriesQuery": {
                  "timeSeriesQueryLanguage": "fetch datastream.googleapis.com/Stream| metric \"datastream.googleapis.com/stream/system_latencies\"| filter (resource.stream_id =~ \"^${var.prefix}.*\") | filter (resource.resource_container == \"${var.prefix}\") | align delta(1m)| every 1m| group_by [resource.stream_id], [value_system_latencies_percentile: percentile(value.system_latencies,95)]"
                },
                "plotType": "LINE",
                "targetAxis": "Y1"
              }
            ],
            "chartOptions": {
              "mode": "COLOR",
              "displayHorizontal": false
            },
            "thresholds": [],
            "yAxis": {
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Datastream: Total Latency",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesQueryLanguage": "fetch datastream.googleapis.com/Stream| metric \"datastream.googleapis.com/stream/total_latencies\"| filter (resource.stream_id =~ \"^${var.prefix}.*\") | filter (resource.resource_container == \"${var.project_id}\") | align delta(1m)| every 1m| group_by [resource.stream_id], [value_total_latencies_percentile: percentile(value.total_latencies,50)]"
                },
                "plotType": "LINE",
                "targetAxis": "Y1"
              },
              {
                "timeSeriesQuery": {
                  "timeSeriesQueryLanguage": "fetch datastream.googleapis.com/Stream| metric \"datastream.googleapis.com/stream/total_latencies\"| filter (resource.stream_id =~ \"^${var.prefix}.*\") | filter (resource.resource_container == \"${var.project_id}\") | align delta(1m)| every 1m| group_by [resource.stream_id], [value_total_latencies_percentile: percentile(value.total_latencies,90)]"
                },
                "plotType": "LINE",
                "targetAxis": "Y1"
              },
              {
                "timeSeriesQuery": {
                  "timeSeriesQueryLanguage": "fetch datastream.googleapis.com/Stream| metric \"datastream.googleapis.com/stream/total_latencies\"| filter (resource.stream_id =~ \"^${var.prefix}.*\") | filter (resource.resource_container == \"${var.project_id}\")| align delta(1m)| every 1m| group_by [resource.stream_id], [value_total_latencies_percentile: percentile(value.total_latencies,95)]"
                },
                "plotType": "LINE",
                "targetAxis": "Y1"
              }
            ],
            "chartOptions": {
              "mode": "COLOR",
              "displayHorizontal": false
            },
            "thresholds": [],
            "yAxis": {
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Datastream: Unsupported Events",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesQueryLanguage": "fetch datastream.googleapis.com/Stream| metric \"datastream.googleapis.com/stream/unsupported_event_count\" | filter (resource.stream_id =~ \"^${var.prefix}.*\")| filter (resource.resource_container == \"${var.project_id}\") | align delta(10m) | every 10m | group_by [resource.stream_id], [value_unsupported_event_count_sum: sum(value.unsupported_event_count)]"
                },
                "plotType": "LINE",
                "targetAxis": "Y1"
              }
            ],
            "chartOptions": {
              "mode": "COLOR",
              "displayHorizontal": false
            },
            "thresholds": [],
            "yAxis": {
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Datastream: Throughput (events/sec)",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesQueryLanguage": "fetch datastream.googleapis.com/Stream | metric \"datastream.googleapis.com/stream/event_count\" | filter (resource.stream_id =~ \"^${var.prefix}.*\")| filter (resource.resource_container == \"${var.project_id}\") | align rate(1m) | every 1m | group_by [resource.stream_id], [value_event_count_sum: sum(value.event_count)]"
                },
                "plotType": "LINE",
                "targetAxis": "Y1"
              }
            ],
            "chartOptions": {
              "mode": "COLOR",
              "displayHorizontal": false
            },
            "thresholds": [],
            "yAxis": {
              "scale": "LINEAR"
            }
          }
        },
        {
          "title": "Dataflow: Worker warning/error log count",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "metric.type=\"logging.googleapis.com/log_entry_count\" resource.type=\"dataflow_job\" metric.label.\"severity\"=monitoring.regex.full_match(\"(ERROR|CRITICAL|WARNING)\") metric.label.\"log\"=monitoring.regex.full_match(\"dataflow.googleapis.com/worker.*\")",
                    "aggregation": {
                      "perSeriesAligner": "ALIGN_SUM",
                      "crossSeriesReducer": "REDUCE_SUM",
                      "groupByFields": [
                        "metric.label.\"severity\"",
                        "metric.label.\"log\"",
                        "resource.label.\"job_name\""
                      ]
                    }
                  }
                },
                "plotType": "STACKED_BAR",
                "targetAxis": "Y1",
                "minAlignmentPeriod": "60s"
              }
            ],
            "chartOptions": {
              "mode": "COLOR",
              "displayHorizontal": false
            },
            "thresholds": [],
            "yAxis": {
              "scale": "LINEAR"
            }
          }
        }
      ]
    }
  })
}

resource "google_monitoring_dashboard" "this" {
  dashboard_json = local.dashboard_json
}