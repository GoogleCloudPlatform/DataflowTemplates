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

// Package generate holds the subcommand that generates files based on terraform provider schemas.
package generate

import (
	"github.com/spf13/cobra"
	"io"
	"os"
)

var (
	w       io.Writer = os.Stdout
	Command           = &cobra.Command{
		Use:   "generate",
		Short: "Generates content based on terraform provider schemas",
	}
)

func fileArgs(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}
	f, err := os.Create(args[0])
	w = f
	return err
}
