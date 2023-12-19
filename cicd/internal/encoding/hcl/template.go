package hcl

import (
	_ "embed"
	"text/template"
)

var (
	// DataflowJob is a template.Template for terraform google_dataflow_job resources. Assumes a metadata.Metadata structure. See https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job.
	DataflowJob = template.Must(template.New("google_dataflow_job").Parse(dataflowJobTmpl))
)

//go:embed dataflow_job.tmpl
var dataflowJobTmpl string
