package query

import (
	"fmt"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/cmd/run-terraform-schema/consts_vars"
	"github.com/spf13/cobra"
	"io"
	"log"
	"os"
)

const (
	providerFlagName = "provider"
	resourceFlagName = "resource"
)

var (
	r io.Reader = os.Stdin

	// TODO(damondouglas): remove GoogleBeta when google_dataflow_flex_template_job generally available
	providers = map[string]string{
		consts_vars.Google:     consts_vars.ProviderGoogleUri,
		consts_vars.GoogleBeta: consts_vars.ProviderGoogleBetaUri,
	}

	// TODO(damondouglas): refactor so that google_dataflow_flex_template_job associates with google
	// 	and not google-beta provider when google_dataflow_flex_template_job generally available
	resources = map[string][]string{
		consts_vars.ProviderGoogleUri:     {consts_vars.ResourceDataflowJob},
		consts_vars.ProviderGoogleBetaUri: {consts_vars.ResourceDataflowFlexTemplateJob},
	}

	Command = &cobra.Command{
		Use:   "describe",
		Short: "Describe terraform provider schemas.",
	}
)

func init() {
	addProviderCommands()
}

func addProviderCommands() {
	for prKey, pr := range providers {
		prCmd := &cobra.Command{
			Use:   prKey,
			Short: fmt.Sprintf("Describe terraform provider %s schema", pr),
		}
		for _, resource := range resources[pr] {
			addResourceCommand(prCmd, pr, resource)
		}
		Command.AddCommand(prCmd)
	}
}

func addResourceCommand(parent *cobra.Command, providerUri string, resource string) {
	cmd := &cobra.Command{
		Use:   fmt.Sprintf("resource [FILE]"),
		Short: fmt.Sprintf("Describe terraform resource %s schema from FILE. Reads from STDIN if no FILE provided.", resource),
		Args:  fileArgs,
		RunE:  runE,
	}
	flags := cmd.Flags()
	flags.String(providerFlagName, providerUri, "")
	flags.String(resourceFlagName, resource, "")
	must(flags.MarkHidden(providerFlagName))
	must(flags.MarkHidden(resourceFlagName))

	parent.AddCommand(cmd)
}

func runE(cmd *cobra.Command, _ []string) error {
	providerFlag := cmd.Flag(providerFlagName)
	if providerFlag == nil {
		return fmt.Errorf("%s is not assigned", providerFlagName)
	}

	resourceFlag := cmd.Flag(resourceFlagName)
	if resourceFlag == nil {
		return fmt.Errorf("%s is not assigned", resourceFlagName)
	}

	provider := providerFlag.Value.String()
	resource := providerFlag.Value.String()

	log.Println(provider, resource)

	return nil
}

func fileArgs(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}

	f, err := os.Open(args[0])
	r = f
	return err
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
