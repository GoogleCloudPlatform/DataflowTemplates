/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package describe

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/cmd/run-terraform-schema/consts_vars"
	tfjson "github.com/hashicorp/terraform-json"
	"github.com/spf13/cobra"
	"io"
	"log"
	"os"
	"time"
)

const (
	providerFlagName = "provider"
	resourceFlagName = "resource"
)

var (
	timeout               = time.Second * 3
	r       io.ReadCloser = os.Stdin
	w                     = os.Stdout

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
		Use:   fmt.Sprintf(fmt.Sprintf("%s [FILE]", resource)),
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

func fileArgs(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}

	f, err := os.Open(args[0])
	r = f
	return err
}

func runE(cmd *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer r.Close()
	defer cancel()

	errChan := make(chan error)

	providerFlag := cmd.Flag(providerFlagName)
	if providerFlag == nil {
		return fmt.Errorf("%s is not assigned", providerFlagName)
	}

	resourceFlag := cmd.Flag(resourceFlagName)
	if resourceFlag == nil {
		return fmt.Errorf("%s is not assigned", resourceFlagName)
	}

	provider := providerFlag.Value.String()
	resource := resourceFlag.Value.String()

	go func() {

		var data map[string]*tfjson.ProviderSchema
		if err := json.NewDecoder(r).Decode(&data); err != nil {
			errChan <- fmt.Errorf("error loading provider schema data, err: %w", err)
		}

		providerData, ok := data[provider]
		if !ok || providerData == nil {
			log.Printf("no data for provider: %s", provider)
			cancel()
		}

		if providerData.ResourceSchemas == nil {
			log.Printf("resource schemas is nil for provider %s\n", provider)
			cancel()
		}

		resourceData, ok := providerData.ResourceSchemas[resource]
		if !ok || resourceData == nil {
			log.Printf("data for provider: %s does not contain resource: %s \n", provider, resource)
			cancel()
		}

		if err := json.NewEncoder(w).Encode(map[string]*tfjson.Schema{
			resource: resourceData,
		}); err != nil {
			errChan <- err
		}

		cancel()

	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		deadline, _ := ctx.Deadline()
		if time.Now().After(deadline) {
			return fmt.Errorf("deadline exceeded. Perhaps FILE not specified or not piped from STDIN")
		}
		return nil
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
