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
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/cmd/run-terraform-schema/describe"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/cmd/run-terraform-schema/download"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/cmd/run-terraform-schema/generate"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "run-terraform-schema",
		Short: "Downloads, describes, and manages tasks related to terraform provider schemas",
	}
)

func init() {
	rootCmd.AddCommand(download.Command, describe.Command, generate.Command)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
