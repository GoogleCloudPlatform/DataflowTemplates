common_params = {
  project = "YOUR_PROJECT_ID"
  region  = "us-central1"
}

dataflow_params = {
  template_params = {
    local_sink_options_file_path = "sqlserver-sink-options.json"
  }
}
