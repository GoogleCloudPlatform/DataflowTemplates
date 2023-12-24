/*
 * Copyright (C) 2022 Google LLC
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
	"io"
	"log"
	"os"
	"path"
)

const (
	googleProviderFileName     = "google.json"
	googleBetaProviderFileName = "google-beta.json"
	registryPathGoogle         = "registry.terraform.io/hashicorp/google"
	registryPathGoogleBeta     = "registry.terraform.io/hashicorp/google-beta"
)

var (
	command = &cobra.Command{
		Use:   "run-get-terraform-schema [DIR]",
		Short: "Acquires the schema of Dataflow related resources of the Google and Google-Beta providers. Outputs to DIR, created if needed; outputs to STDOUT if no DIR provided.",
		Args:  parseTargetDir,
		RunE:  runE,
	}

	output = map[string]io.Writer{
		registryPathGoogle:     os.Stdout,
		registryPathGoogleBeta: os.Stdout,
	}
)

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

	googleProviderFilePath := path.Join(targetDir, googleProviderFileName)
	googleBetaProviderFilePath := path.Join(targetDir, googleBetaProviderFileName)

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
