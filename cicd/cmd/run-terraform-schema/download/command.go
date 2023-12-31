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

// Package download holds the subcommand that fetches the terraform provider schemas.
package download

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/cmd/run-terraform-schema/consts_vars"
	"github.com/hashicorp/go-version"
	install "github.com/hashicorp/hc-install"
	"github.com/hashicorp/hc-install/fs"
	"github.com/hashicorp/hc-install/product"
	"github.com/hashicorp/hc-install/releases"
	"github.com/hashicorp/hc-install/src"
	"github.com/hashicorp/terraform-exec/tfexec"
	"github.com/spf13/cobra"
	"io"
	"log"
	"os"
	"path"
)

const (
	providerFileName  = "main.tf"
	providerTmplStr   = `provider "%s" {}`
	versionConstraint = "~> 1.0"
)

var (
	Command = &cobra.Command{
		Use: "download [FILE]",
		Short: `Acquires the schema of terraform providers.
Outputs to FILE, created if needed; outputs to STDOUT if no FILE provided.`,
		Args:     fileArg,
		PreRunE:  d.preRunE,
		RunE:     runE,
		PostRunE: d.postRunE,
	}

	providers = []string{
		consts_vars.ProviderGoogleUri,
		consts_vars.ProviderGoogleBetaUri,
	}

	d = &downloader{}

	w                  = os.Stdout
	googleProvider     = fmt.Sprintf(providerTmplStr, consts_vars.Google)
	googleBetaProvider = fmt.Sprintf(providerTmplStr, consts_vars.GoogleBeta)
	content            = fmt.Sprintf("%s\n%s\n", googleProvider, googleBetaProvider)
)

func init() {
	Command.Flags().StringSliceVarP(&providers, "providers", "p", providers, "Providers to download from the registry.")
}

func fileArg(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}

	f, err := os.Create(args[0])
	w = f

	return err
}

func runE(cmd *cobra.Command, _ []string) error {
	pr, err := d.cli.ProvidersSchema(cmd.Context())
	if err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(pr.Schemas)
}

type downloader struct {
	installer  *install.Installer
	execPath   string
	workingDir string
	cli        *tfexec.Terraform
}

func (d *downloader) preRunE(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	if err := d.ensureTerraformCli(); err != nil {
		return fmt.Errorf("error ensuring terraform executable: %w", err)
	}
	if err := d.stageProvider(); err != nil {
		return fmt.Errorf("error staging provider: %w", err)
	}
	if err := d.initTerraform(ctx); err != nil {
		return fmt.Errorf("error initializing terraform: %s", err)
	}

	return nil
}

// ensureTerraformCli in os path. Instantiates an install.Installer to check the current os or install terraform binary.
func (d *downloader) ensureTerraformCli() error {
	log.Printf("initalizing terraform installer for %s\n", versionConstraint)
	d.installer = install.NewInstaller()

	v1 := version.MustConstraints(version.NewConstraint(versionConstraint))

	execPath, err := d.installer.Ensure(context.Background(), []src.Source{
		&fs.Version{
			Product:     product.Terraform,
			Constraints: v1,
		},
		&releases.LatestVersion{
			Product:     product.Terraform,
			Constraints: v1,
		},
	})

	d.execPath = execPath

	return err
}

// stageProvider by writing to a temporary directory the provider resource block configuring both Google and Google-Beta
// providers. This is needed by terraform to execute its provider subcommand.
func (d *downloader) stageProvider() error {
	log.Println("staging provider")

	workingDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("error creating temp directory: %w", err)
	}

	d.workingDir = workingDir

	filePath := path.Join(d.workingDir, providerFileName)

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating %s: %w", filePath, err)
	}

	log.Printf("created file in %s", filePath)

	if _, err := io.WriteString(f, content); err != nil {
		return fmt.Errorf("error writing provider content to %s: %w", filePath, err)
	}

	log.Printf("wrote provider configuration to %s", filePath)

	return nil
}

// initTerraform initializes the terraform workflow needed to reflect the provider schema.
func (d *downloader) initTerraform(ctx context.Context) error {
	log.Printf("initializing terraform in workingDir: %s and execPath: %s\n", d.workingDir, d.execPath)

	cli, err := tfexec.NewTerraform(d.workingDir, d.execPath)
	if err != nil {
		return fmt.Errorf("error intantiating terraform in workingDir: %s and execPath: %s, err %w", d.workingDir, d.execPath)
	}

	d.cli = cli

	if err = d.cli.Init(ctx); err != nil {
		return err
	}
	log.Println("initiated terraform module")

	return nil
}

func (d *downloader) postRunE(cmd *cobra.Command, _ []string) error {
	if err := os.RemoveAll(d.workingDir); err != nil {
		return err
	}

	return d.installer.Remove(cmd.Context())
}
