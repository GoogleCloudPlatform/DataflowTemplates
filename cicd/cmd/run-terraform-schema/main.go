package main

import (
	"github.com/spf13/cobra"
	"path"
)

const (
	google                          = "google"
	googleBeta                      = "google-beta"
	hashicorpRegistry               = "registry.terraform.io/hashicorp"
	resourceDataflowJob             = "google_dataflow_job"
	resourceDataflowFlexTemplateJob = "google_dataflow_flex_template_job"
)

var (
	registry  = hashicorpRegistry
	providers = []string{
		path.Join(hashicorpRegistry, google),
		path.Join(hashicorpRegistry, googleBeta),
	}
	resources = []string{
		resourceDataflowFlexTemplateJob,
		resourceDataflowJob,
	}

	rootCmd = &cobra.Command{
		Use: "run-get-terraform-schema [FILE]",
		Short: `Acquires the schema of terraform providers.
Outputs to DIR, created if needed; outputs to STDOUT if no DIR provided.`,
	}
)

func main() {

}
