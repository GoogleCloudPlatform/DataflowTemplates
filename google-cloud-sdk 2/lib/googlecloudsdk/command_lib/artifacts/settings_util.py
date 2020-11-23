# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utility for forming settings for Artifacts Registry repositories."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import base64
import json

from googlecloudsdk.api_lib.artifacts import exceptions as ar_exceptions
from googlecloudsdk.api_lib.auth import service_account
from googlecloudsdk.command_lib.artifacts import requests as ar_requests
from googlecloudsdk.command_lib.artifacts import util as ar_util
from googlecloudsdk.core import config
from googlecloudsdk.core import properties
from googlecloudsdk.core.console import console_io
from googlecloudsdk.core.credentials import creds
from googlecloudsdk.core.credentials import store
from googlecloudsdk.core.util import encoding
from googlecloudsdk.core.util import files

_PROJECT_NOT_FOUND_ERROR = """\
Failed to find attribute [project]. \
The attribute can be set in the following ways:
- provide the argument [--project] on the command line
- set the property [core/project]"""

_REPO_NOT_FOUND_ERROR = """\
Failed to find attribute [repository]. \
The attribute can be set in the following ways:
- provide the argument [--repository] on the command line
- set the property [artifacts/repository]"""

_LOCATION_NOT_FOUND_ERROR = """\
Failed to find attribute [location]. \
The attribute can be set in the following ways:
- provide the argument [--location] on the command line
- set the property [artifacts/location]"""


def _GetRequiredProjectValue(args):
  if not args.project and not properties.VALUES.core.project.Get():
    raise ar_exceptions.InvalidInputValueError(_PROJECT_NOT_FOUND_ERROR)
  return ar_util.GetProject(args)


def _GetRequiredRepoValue(args):
  if not args.repository and not properties.VALUES.artifacts.repository.Get():
    raise ar_exceptions.InvalidInputValueError(_REPO_NOT_FOUND_ERROR)
  return ar_util.GetRepo(args)


def _GetRequiredLocationValue(args):
  if not args.location and not properties.VALUES.artifacts.location.Get():
    raise ar_exceptions.InvalidInputValueError(_LOCATION_NOT_FOUND_ERROR)
  return ar_util.GetLocation(args)


def _GetLocationAndRepoPath(args, repo_format):
  """Get resource values and validate user input."""
  repo = _GetRequiredRepoValue(args)
  project = _GetRequiredProjectValue(args)
  location = _GetRequiredLocationValue(args)
  repo_path = project + "/" + repo
  location_list = ar_requests.ListLocations(project)
  if location.lower() not in location_list:
    raise ar_exceptions.UnsupportedLocationError(
        "{} is not a valid location. Valid locations are [{}].".format(
            location, ", ".join(location_list)))
  repo = ar_requests.GetRepository(
      "projects/{}/locations/{}/repositories/{}".format(project, location,
                                                        repo))
  if repo.format != repo_format:
    raise ar_exceptions.InvalidInputValueError(
        "Invalid repository type {}. Valid type is {}.".format(
            repo.format, repo_format))
  return location, repo_path


def _LoadJsonFile(filename):
  """Checks and validates if given filename is a proper JSON file.

  Args:
    filename: str, path to JSON file.

  Returns:
    bytes, the content of the file.
  """
  content = console_io.ReadFromFileOrStdin(filename, binary=True)
  try:
    json.loads(encoding.Decode(content))
    return content
  except ValueError as e:
    if filename.endswith(".json"):
      raise service_account.BadCredentialFileException(
          "Could not read JSON file {0}: {1}".format(filename, e))
  raise service_account.BadCredentialFileException(
      "Unsupported credential file: {0}".format(filename))


def _GetServiceAccountCreds(args):
  """Gets service account credentials from given file path or default if any.

  Args:
    args: Command arguments.

  Returns:
    str, service account credentials.
  """
  if args.json_key:
    file_content = _LoadJsonFile(args.json_key)
    return base64.b64encode(file_content).decode("utf-8")

  account = properties.VALUES.core.account.Get()
  if not account:
    raise store.NoActiveAccountException()
  cred = store.Load(account, prevent_refresh=True, use_google_auth=True)
  if not cred:
    raise store.NoCredentialsForAccountException(account)

  if _IsServiceAccountCredentials(cred):
    paths = config.Paths()
    json_content = files.ReadFileContents(
        paths.LegacyCredentialsAdcPath(account))
    return base64.b64encode(json_content.encode("utf-8")).decode("utf-8")
  return ""


def _IsServiceAccountCredentials(cred):
  if creds.IsOauth2ClientCredentials(cred):
    return creds.CredentialType.FromCredentials(
        cred) == creds.CredentialType.SERVICE_ACCOUNT
  else:
    return creds.CredentialTypeGoogleAuth.FromCredentials(
        cred) == creds.CredentialTypeGoogleAuth.SERVICE_ACCOUNT


def GetNpmSettingsSnippet(args):
  """Forms an npm settings snippet to add to the .npmrc file.

  Args:
    args: an argparse namespace. All the arguments that were provided to this
      command invocation.

  Returns:
    An npm settings snippet.
  """
  messages = ar_requests.GetMessages()
  location, repo_path = _GetLocationAndRepoPath(
      args, messages.Repository.FormatValueValuesEnum.NPM)
  registry_path = "{location}-npm.pkg.dev/{repo_path}/".format(**{
      "location": location,
      "repo_path": repo_path
  })
  configured_registry = "registry"
  if args.scope:
    if not args.scope.startswith("@") or len(args.scope) <= 1:
      raise ar_exceptions.InvalidInputValueError(
          "Scope name must start with \"@\" and be longer than 1 character.")
    configured_registry = args.scope + ":" + configured_registry

  data = {
      "configured_registry": configured_registry,
      "registry_path": registry_path,
      "repo_path": repo_path
  }

  sa_creds = _GetServiceAccountCreds(args)
  if sa_creds:
    npm_setting_template = """\
# Insert following snippet into your .npmrc

{configured_registry}=https://{registry_path}
//{registry_path}:_password="{password}"
//{registry_path}:username=_json_key_base64
//{registry_path}:email=not.valid@email.com
//{registry_path}:always-auth=true
"""
    data["password"] = base64.b64encode(
        sa_creds.encode("utf-8")).decode("utf-8")
  else:
    npm_setting_template = """\
# Insert following snippet into your .npmrc

{configured_registry}=https://{registry_path}
//{registry_path}:_authToken=""
//{registry_path}:always-auth=true
"""

  return npm_setting_template.format(**data)


def GetMavenSnippet(args):
  """Forms a maven snippet to add to the pom.xml file.

  Args:
    args: an argparse namespace. All the arguments that were provided to this
      command invocation.

  Returns:
    A maven snippet.
  """
  messages = ar_requests.GetMessages()
  location, repo_path = _GetLocationAndRepoPath(
      args, messages.Repository.FormatValueValuesEnum.MAVEN)
  data = {
      "scheme":
          "artifactregistry",
      "build_ext":
          """\n
<build>
  <extensions>
    <extension>
      <groupId>com.google.cloud.artifactregistry</groupId>
      <artifactId>artifactregistry-maven-wagon</artifactId>
      <version>2.1.0</version>
    </extension>
  </extensions>
</build>""",
      "location":
          location,
      "server_id":
          "artifact-registry",
      "repo_path":
          repo_path,
  }
  mvn_template = """\
<!-- Insert following snippet into your pom.xml -->

<project>
  <distributionManagement>
    <snapshotRepository>
      <id>{server_id}</id>
      <url>{scheme}://{location}-maven.pkg.dev/{repo_path}</url>
    </snapshotRepository>
    <repository>
      <id>{server_id}</id>
      <url>{scheme}://{location}-maven.pkg.dev/{repo_path}</url>
    </repository>
  </distributionManagement>

  <repositories>
    <repository>
      <id>{server_id}</id>
      <url>{scheme}://{location}-maven.pkg.dev/{repo_path}</url>
      <releases>
        <enabled>true</enabled>
      </releases>
      <snapshots>
        <enabled>true</enabled>
      </snapshots>
    </repository>
  </repositories>{build_ext}
</project>
"""

  sa_creds = _GetServiceAccountCreds(args)
  if sa_creds:
    mvn_template += """
<!-- Insert following snippet into your settings.xml -->

<settings>
  <servers>
    <server>
      <id>{server_id}</id>
      <configuration>
        <httpConfiguration>
          <get>
            <usePreemptive>true</usePreemptive>
          </get>
          <head>
            <usePreemptive>true</usePreemptive>
          </head>
          <put>
            <params>
              <property>
                <name>http.protocol.expect-continue</name>
                <value>false</value>
              </property>
            </params>
          </put>
        </httpConfiguration>
      </configuration>
      <username>{username}</username>
      <password>{password}</password>
    </server>
  </servers>
</settings>
"""

    data["scheme"] = "https"
    data["build_ext"] = ""
    data["username"] = "_json_key_base64"
    data["password"] = sa_creds

  return mvn_template.format(**data)


def GetGradleSnippet(args):
  """Forms a gradle snippet to add to the build.gradle file.

  Args:
    args: an argparse namespace. All the arguments that were provided to this
      command invocation.

  Returns:
    A gradle snippet.
  """
  messages = ar_requests.GetMessages()
  location, repo_path = _GetLocationAndRepoPath(
      args, messages.Repository.FormatValueValuesEnum.MAVEN)
  data = {"location": location, "repo_path": repo_path}

  sa_creds = _GetServiceAccountCreds(args)
  if sa_creds:
    gradle_template = """\
// Move the secret to ~/.gradle.properties
def artifactRegistryMavenSecret = "{password}"

// Insert following snippet into your build.gradle
// see docs.gradle.org/current/userguide/publishing_maven.html

plugins {{
  id "maven-publish"
}}

publishing {{
  repositories {{
    maven {{
      url "https://{location}-maven.pkg.dev/{repo_path}"
      credentials {{
        username = "{username}"
        password = "$artifactRegistryMavenSecret"
      }}
    }}
  }}
}}

repositories {{
  maven {{
    url "https://{location}-maven.pkg.dev/{repo_path}"
    credentials {{
      username = "{username}"
      password = "$artifactRegistryMavenSecret"
    }}
    authentication {{
      basic(BasicAuthentication)
    }}
  }}
}}
"""

    data["username"] = "_json_key_base64"
    data["password"] = sa_creds

  else:
    gradle_template = """\
// Insert following snippet into your build.gradle
// see docs.gradle.org/current/userguide/publishing_maven.html

plugins {{
  id "maven-publish"
  id "com.google.cloud.artifactregistry.gradle-plugin" version "{extension_version}"
}}

publishing {{
  repositories {{
    maven {{
      url "artifactregistry://{location}-maven.pkg.dev/{repo_path}"
    }}
  }}
}}

repositories {{
  maven {{
    url "artifactregistry://{location}-maven.pkg.dev/{repo_path}"
  }}
}}
"""

    data["extension_version"] = "2.1.0"
  return gradle_template.format(**data)
