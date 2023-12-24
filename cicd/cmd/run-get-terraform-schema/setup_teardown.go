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
	"context"
	"fmt"
	"github.com/hashicorp/go-version"
	install "github.com/hashicorp/hc-install"
	"github.com/hashicorp/hc-install/fs"
	"github.com/hashicorp/hc-install/product"
	"github.com/hashicorp/hc-install/releases"
	"github.com/hashicorp/hc-install/src"
	"github.com/hashicorp/terraform-exec/tfexec"
	"io"
	"log"
	"os"
	"path"
)

const (
	google            = "google"
	googleBeta        = "google-beta"
	providerTmplStr   = `provider "%s" {}`
	fileName          = "main.tf"
	versionConstraint = "~> 1.0"
)

var (
	googleProvider     = fmt.Sprintf(providerTmplStr, google)
	googleBetaProvider = fmt.Sprintf(providerTmplStr, googleBeta)
	content            = fmt.Sprintf("%s\n%s\n", googleProvider, googleBetaProvider)
)

// setup the workflow:
// 1. ensure terraform executable exists in os path
// 2. stage the provider HCL files required by terraform to get the schema
// 3. initialize terraform as required to reflect on provider metadata
func (t *terraform) setup(ctx context.Context) error {
	if err := t.ensureTerraformCli(); err != nil {
		return fmt.Errorf("error ensuring terraform executable: %w", err)
	}
	if err := t.stageProvider(); err != nil {
		return fmt.Errorf("error staging provider: %w", err)
	}
	if err := t.initTerraform(ctx); err != nil {
		return fmt.Errorf("error initializing terraform: %s", err)
	}

	return nil
}

// teardown the workflow:
// 1. remove all temporary files
// 2. remove the terraform installer
func (t *terraform) teardown(ctx context.Context) error {
	if err := os.RemoveAll(t.workingDir); err != nil {
		return err
	}
	return t.installer.Remove(ctx)
}

// ensureTerraformCli in os path. Instantiates an install.Installer to check the current os or install terraform binary.
func (t *terraform) ensureTerraformCli() error {
	var err error

	log.Printf("initalizing terraform installer for %s\n", versionConstraint)
	t.installer = install.NewInstaller()

	v1 := version.MustConstraints(version.NewConstraint(versionConstraint))

	t.execPath, err = t.installer.Ensure(context.Background(), []src.Source{
		&fs.Version{
			Product:     product.Terraform,
			Constraints: v1,
		},
		&releases.LatestVersion{
			Product:     product.Terraform,
			Constraints: v1,
		},
	})

	return err
}

// stageProvider by writing to a temporary directory the provider resource block configuring both Google and Google-Beta
// providers. This is needed by terraform to execute its provider subcommand.
func (t *terraform) stageProvider() error {
	var err error

	log.Println("staging provider")

	if t.workingDir, err = os.MkdirTemp("", ""); err != nil {
		return fmt.Errorf("error creating temp directory: %w", err)
	}

	filePath := path.Join(t.workingDir, fileName)

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

// initTerraform initializes the terraform workflow needed to reflect the provider schema
func (t *terraform) initTerraform(ctx context.Context) error {
	var err error

	log.Printf("initializing terraform in workingDir: %s and execPath: %s\n", t.workingDir, t.execPath)

	if t.cli, err = tfexec.NewTerraform(t.workingDir, t.execPath); err != nil {
		return fmt.Errorf("error intantiating terraform in workingDir: %s and execPath: %s, err %w", t.workingDir, t.execPath)
	}

	if err = t.cli.Init(ctx); err != nil {
		return err
	}
	log.Println("initiated terraform module")

	return nil
}
