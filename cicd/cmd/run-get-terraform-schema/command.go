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

package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"path"
)

var (
	command = &cobra.Command{
		Use: "run-get-terraform-schema [DIR]",
		Short: `Acquires the schema of Dataflow related resources of the Google and Google-Beta providers.
Outputs to DIR, created if needed; outputs to STDOUT if no DIR provided.`,
		Long: `Acquires the schema of Dataflow related resources of the Google and Google-Beta providers.

DIR is an optional positional argument that configures the output directory. If not DIR is specified, then the
program outputs to STDOUT.

Available formats:
	tf	Outputs in a terraform hcl format for use as a freemarker template.
	json	Outputs the terraform provider schema in JSON format.
`,
		Args:    parseTargetDir,
		RunE:    runE,
		PreRunE: preRunE,
	}
)

func init() {
	command.Flags().StringVarP(&format, "format", "f", format, formatHelp)
}

func preRunE(_ *cobra.Command, _ []string) error {
	if format == "" {
		return fmt.Errorf("format is empty but required")
	}
	log.Printf("using format: %s\n", format)

	switch format {
	case formatJson:
		formatterMap = map[string]formatter{
			registryPathGoogle:     &jsonFormatter{},
			registryPathGoogleBeta: &jsonFormatter{},
		}
		return nil
	case formatTerraform:
		formatterMap = map[string]formatter{
			registryPathGoogle: &hclFormatter{
				resourceKey: resourceDataflowJob,
			},
			registryPathGoogleBeta: &hclFormatter{
				resourceKey: resourceDataflowFlexTemplateJob,
			},
		}
		return nil
	default:
		return fmt.Errorf("specified format: %s is not allowed; run with -help to see allowed formats", format)
	}
}

func runE(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	tf := &terraform{}
	if err := tf.setup(ctx); err != nil {
		return err
	}
	if err := tf.getSchema(ctx); err != nil {
		return err
	}
	if err := tf.teardown(ctx); err != nil {
		return err
	}
	return nil
}

func parseTargetDir(_ *cobra.Command, args []string) error {
	var err error

	if len(args) == 0 {
		return nil
	}

	targetDir := args[0]

	if err = os.MkdirAll(targetDir, os.ModeDir); err != nil {
		return fmt.Errorf("error creating directory: %s", targetDir)
	}

	googleProviderFilePath := path.Join(targetDir, fmt.Sprintf("%s.%s", google, format))
	googleBetaProviderFilePath := path.Join(targetDir, fmt.Sprintf("%s.%s", googleBeta, format))

	if output[registryPathGoogle], err = os.Create(googleProviderFilePath); err != nil {
		return fmt.Errorf("error creating %s: %w", googleProviderFilePath, err)
	}
	log.Printf("created %s\n", googleProviderFilePath)

	if output[registryPathGoogleBeta], err = os.Create(googleBetaProviderFilePath); err != nil {
		return fmt.Errorf("error creating %s: %w", googleBetaProviderFilePath, err)
	}
	log.Printf("created %s\n", googleBetaProviderFilePath)

	return nil
}
