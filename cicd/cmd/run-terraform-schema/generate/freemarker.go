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

package generate

import (
	"encoding/json"
	"fmt"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/cmd/run-terraform-schema/consts_vars"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/terraform"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/terraform/schemas"
	tfjson "github.com/hashicorp/terraform-json"
	"github.com/spf13/cobra"
)

var (
	provider     string
	resource     string
	in           []byte
	includeExtra bool

	freemarkerCmd = &cobra.Command{
		Use:   "freemarker",
		Short: "Generates freemarker templates for generating terraform",
	}

	googleCmd = &cobra.Command{
		Use:   consts_vars.Google,
		Short: "Generates freemarker templates for generating terraform based on the Google provider resources",
	}

	dataflowJob = &cobra.Command{
		Use:     fmt.Sprintf("%s [FILE]", consts_vars.ResourceDataflowJob),
		Short:   "Generates freemarker templates for generating terraform that creates a Dataflow Job; Outputs to FILE or STDOUT if not provided.",
		Args:    fileArgs,
		PreRunE: freemarkerPreRunE,
		RunE:    freemarkerRunE,
	}

	googleBetaCmd = &cobra.Command{
		Use:   consts_vars.GoogleBeta,
		Short: "Generates freemarker templates for generating terraform based on the Google Beta provider resources",
	}

	dataflowFlexJob = &cobra.Command{
		Use:     fmt.Sprintf("%s [FILE]", consts_vars.ResourceDataflowFlexTemplateJob),
		Short:   "Generates freemarker templates for generating terraform that creates a Dataflow Flex Job; Outputs to FILE or STDOUT if not provided.",
		Args:    fileArgs,
		PreRunE: freemarkerPreRunE,
		RunE:    freemarkerRunE,
	}

	providerSchemas = map[string][]byte{
		consts_vars.Google:     schemas.Google,
		consts_vars.GoogleBeta: schemas.GoogleBeta,
	}

	providerExtras = map[string]terraform.Extra{
		consts_vars.Google:     terraform.GoogleProviderExtra,
		consts_vars.GoogleBeta: terraform.GoogleProviderBetaExtra,
	}

	templatePathExtras = map[string]terraform.Extra{
		consts_vars.ResourceDataflowJob:             terraform.TemplatePathClassic,
		consts_vars.ResourceDataflowFlexTemplateJob: terraform.TemplatePathFlex,
	}

	// Ignores schema attributes with these names.
	attrMatcher = &terraform.And[*tfjson.SchemaAttribute]{
		terraform.Not[*tfjson.SchemaAttribute](&terraform.Or[*tfjson.SchemaAttribute]{
			terraform.MatchName[*tfjson.SchemaAttribute]("container_spec_gcs_path"),
			terraform.MatchName[*tfjson.SchemaAttribute]("effective_labels"),
			terraform.MatchName[*tfjson.SchemaAttribute]("id"),
			terraform.MatchName[*tfjson.SchemaAttribute]("job_id"),
			terraform.MatchName[*tfjson.SchemaAttribute]("on_delete"),
			terraform.MatchName[*tfjson.SchemaAttribute]("parameters"),
			terraform.MatchName[*tfjson.SchemaAttribute]("project"),
			terraform.MatchName[*tfjson.SchemaAttribute]("region"),
			terraform.MatchName[*tfjson.SchemaAttribute]("state"),
			terraform.MatchName[*tfjson.SchemaAttribute]("template_gcs_path"),
			terraform.MatchName[*tfjson.SchemaAttribute]("terraform_labels"),
			terraform.MatchName[*tfjson.SchemaAttribute]("transform_name_mapping"),
			terraform.MatchName[*tfjson.SchemaAttribute]("type"),
		}),
		terraform.MatchIsDeprecated(false),
	}
)

func init() {
	googleCmd.AddCommand(dataflowJob)
	googleBetaCmd.AddCommand(dataflowFlexJob)
	freemarkerCmd.AddCommand(googleCmd, googleBetaCmd)
	Command.AddCommand(freemarkerCmd)

	Command.PersistentFlags().BoolVar(&includeExtra, "include_extra", true, "Flags whether to include the freemarker snippets")
}

func freemarkerPreRunE(cmd *cobra.Command, _ []string) error {
	resource = cmd.Name()
	provider = cmd.Parent().Name()
	if resource == "" || provider == "" {
		return fmt.Errorf("required but empty: resource: %s, provider: %s", resource, provider)
	}
	schema, ok := providerSchemas[provider]
	if !ok {
		return fmt.Errorf("schema not known for provider: %s", provider)
	}

	in = schema

	return nil
}

func freemarkerRunE(_ *cobra.Command, _ []string) error {
	var data map[string]*tfjson.Schema

	if err := json.Unmarshal(in, &data); err != nil {
		return fmt.Errorf("error decoding input into %T, err %w", data, err)
	}

	nameMatcher := terraform.MatchName[*tfjson.Schema](resource)
	dataFilter := terraform.NewFilter[*tfjson.Schema](nameMatcher)
	attrFilter := terraform.NewFilter[*tfjson.SchemaAttribute](attrMatcher)

	data = filter(data, dataFilter, attrFilter)

	var extra []terraform.Extra
	providerExtra, ok := providerExtras[provider]
	if !ok {
		return fmt.Errorf("could not acquire template extra from provider: %s", provider)
	}

	templatePathExtra, ok := templatePathExtras[resource]
	if !ok {
		return fmt.Errorf("could not acquire template extra from resource: %s", resource)
	}

	extra = append(extra, providerExtra, templatePathExtra)

	if includeExtra {
		extra = append(extra, terraform.FreemarkerPreResourceExtra, terraform.FreemarkerResourceExtra)
	}

	enc := terraform.ModuleEncoder(extra...)

	return enc.Encode(w, data)
}

func filter(data map[string]*tfjson.Schema, dataFilter *terraform.Filter[*tfjson.Schema], attrFilter *terraform.Filter[*tfjson.SchemaAttribute]) map[string]*tfjson.Schema {
	result := map[string]*tfjson.Schema{}
	data = dataFilter.Apply(data)
	for schemaK, schemaV := range data {
		block := schemaV.Block
		resultV := &tfjson.Schema{
			Version: schemaV.Version,
			Block: &tfjson.SchemaBlock{
				Attributes:      map[string]*tfjson.SchemaAttribute{},
				NestedBlocks:    block.NestedBlocks,
				Description:     block.Description,
				DescriptionKind: block.DescriptionKind,
				Deprecated:      block.Deprecated,
			},
		}
		attrs := attrFilter.Apply(block.Attributes)
		for attrK, attrV := range attrs {
			resultV.Block.Attributes[attrK] = attrV
		}
		result[schemaK] = resultV
	}
	return result
}
