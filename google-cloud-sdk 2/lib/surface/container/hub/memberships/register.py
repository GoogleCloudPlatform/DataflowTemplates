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
"""The register command for registering a clusters with the Hub."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
import textwrap

from apitools.base.py import exceptions as apitools_exceptions
from googlecloudsdk.api_lib.util import exceptions as core_api_exceptions
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.container.hub import agent_util
from googlecloudsdk.command_lib.container.hub import api_util
from googlecloudsdk.command_lib.container.hub import exclusivity_util
from googlecloudsdk.command_lib.container.hub import kube_util
from googlecloudsdk.command_lib.container.hub import util as hub_util
from googlecloudsdk.command_lib.util.apis import arg_utils
from googlecloudsdk.core import exceptions
from googlecloudsdk.core import log
from googlecloudsdk.core.console import console_io
from googlecloudsdk.core.util import files

SERVICE_ACCOUNT_KEY_FILE_FLAG = '--service-account-key-file'
DOCKER_CREDENTIAL_FILE_FLAG = '--docker-credential-file'


class Register(base.CreateCommand):
  r"""Register a cluster with Hub.

  This command registers a cluster with the Hub by:

    1. Creating a Hub Membership resource corresponding to the cluster.
    2. Adding in-cluster Kubernetes Resources that make the cluster exclusive
       to one Hub.
    3. Installing the Connect Agent into this cluster.

  A successful registration implies that the cluster is now exclusive to a
  single Hub.

  For more information about Connect Agent, go to:
  https://cloud.google.com/anthos/multicluster-management/connect/overview/

  To register a non-GKE or GKE On-Prem cluster use --context flag (with an
  optional --kubeconfig flag).

  To register a GKE cluster use --gke-cluster or --gke-uri flag (no --kubeconfig
  flag is required).

  In all cases, the Connect Agent that is installed in the target cluster must
  authenticate to Google using a `--service-account-key-file` that corresponds
  to a service account that has been granted `gkehub.connect` permissions.

  If the cluster is already registered to another Hub, the registration is not
  successful.

  Rerunning this command against the same cluster with the same CLUSTER_NAME and
  target GKEHub is successful and upgrades the Connect Agent if a new agent is
  available.

  ## EXAMPLES

    Register a non-GKE or GKE On-Prem cluster referenced from a specific
    kubeconfig file, and install the Connect Agent:

      $ {command} my-cluster \
        --context=my-cluster-context \
        --kubeconfig=/home/user/custom_kubeconfig \
        --service-account-key-file=/tmp/keyfile.json

    Register a non-GKE or GKE On-Prem cluster referenced from the default
    kubeconfig file, and install the Connect Agent:

      $ {command} my-cluster \
        --context=my-cluster-context \
        --service-account-key-file=/tmp/keyfile.json

    Register a non-GKE or GKE On-Prem cluster, and install a specific version
    of the Connect Agent:

      $ {command} my-cluster \
        --context=my-cluster-context \
        --version=gkeconnect_20190802_02_00 \
        --service-account-key-file=/tmp/keyfile.json

    Register a non-GKE or GKE On-Prem cluster and output a manifest that can be
    used to install the Connect Agent:

      $ {command} my-cluster \
        --context=my-cluster-context \
        --manifest-output-file=/tmp/manifest.yaml \
        --service-account-key-file=/tmp/keyfile.json

    Register a GKE cluster referenced from a GKE URI, and install the Connect
    Agent:

      $ {command} my-cluster \
        --gke-uri=my-cluster-gke-uri \
        --service-account-key-file=/tmp/keyfile.json

    Register a GKE cluster referenced from a GKE Cluster location and name, and
    install the Connect Agent:

      $ {command} my-cluster \
        --gke-cluster=my-cluster-region-or-zone/my-cluster \
        --service-account-key-file=/tmp/keyfile.json

    Register a GKE cluster, and install a specific version of the Connect
    Agent:

      $ {command} my-cluster \
        --gke-uri=my-cluster-gke-uri \
        --version=gkeconnect_20190802_02_00 \
        --service-account-key-file=/tmp/keyfile.json

      $ {command} my-cluster \
        --gke-cluster=my-cluster-region-or-zone/my-cluster \
        --version=gkeconnect_20190802_02_00 \
        --service-account-key-file=/tmp/keyfile.json

    Register a GKE cluster and output a manifest that can be used to install the
    Connect Agent:

      $ {command} my-cluster \
        --gke-uri=my-cluster-gke-uri \
        --manifest-output-file=/tmp/manifest.yaml \
        --service-account-key-file=/tmp/keyfile.json

      $ {command} my-cluster \
        --gke-cluster=my-cluster-region-or-zone/my-cluster \
        --manifest-output-file=/tmp/manifest.yaml \
        --service-account-key-file=/tmp/keyfile.json
  """

  @classmethod
  def Args(cls, parser):
    parser.add_argument(
        'CLUSTER_NAME',
        type=str,
        help=textwrap.dedent("""\
          The membership name that you choose to uniquely represents the cluster
          being registered on the Hub.
         """),
    )
    hub_util.AddUnRegisterCommonArgs(parser)
    parser.add_argument(
        '--manifest-output-file',
        type=str,
        help=textwrap.dedent("""\
            The full path of the file into which the Connect Agent installation
            manifest should be stored. If this option is provided, then the
            manifest will be written to this file and will not be deployed into
            the cluster by gcloud, and it will need to be deployed manually.
          """),
    )
    parser.add_argument(
        '--proxy',
        type=str,
        help=textwrap.dedent("""\
            The proxy address in the format of http[s]://{hostname}. The proxy
            must support the HTTP CONNECT method in order for this connection to
            succeed.
          """),
    )
    parser.add_argument(
        '--version',
        type=str,
        hidden=True,
        help=textwrap.dedent("""\
          The version of the Connect Agent to install/upgrade if not using the
          latest connect version.
          """),
    )
    parser.add_argument(
        DOCKER_CREDENTIAL_FILE_FLAG,
        type=str,
        hidden=True,
        help=textwrap.dedent("""\
          The credentials to be used if a private registry is provided and auth
          is required. The contents of the file will be stored into a Secret and
          referenced from the imagePullSecrets of the Connect Agent workload.
          """),
    )
    parser.add_argument(
        '--docker-registry',
        type=str,
        hidden=True,
        help=textwrap.dedent("""\
        The registry to pull GKE Connect Agent image if not using gcr.io/gkeconnect.
          """),
    )
    credentials = parser.add_mutually_exclusive_group(
        required=True)
    credentials.add_argument(
        SERVICE_ACCOUNT_KEY_FILE_FLAG,
        type=str,
        help=textwrap.dedent("""\
            The JSON file of a Google Cloud service account private key. This
            service account key is stored as a secret named ``creds-gcp'' in
            gke-connect namespace. To update the ``creds-gcp'' secret in
            gke-connect namespace with a new service account key file, run the
            following command:

            kubectl delete secret creds-gcp -n gke-connect

            kubectl create secret generic creds-gcp -n gke-connect --from-file=creds-gcp.json=/path/to/file
         """),
    )

    if cls.ReleaseTrack() is not base.ReleaseTrack.GA:
      # Optional groups with required arguments are "modal,"
      # meaning that if any of the required arguments is specified,
      # all are required.
      workload_identity = credentials.add_group(help='Workload Identity')
      workload_identity.add_argument(
          '--enable-workload-identity',
          required=True,
          action='store_true',
          help=textwrap.dedent("""\
            Enable Workload Identity when registering the cluster with Hub.
            Requires gcloud alpha or beta.
            --service_account_key_file flag should not be set if this is set.
            """),
      )
      workload_identity_mutex = workload_identity.add_group(mutex=True)
      workload_identity_mutex.add_argument(
          '--public-issuer-url',
          type=str,
          help=textwrap.dedent("""\
            Skip auto-discovery and register the cluster with this issuer URL.
            Use this option when the OpenID Provider Configuration and associated
            JSON Web Key Set for validating the cluster's service account JWTs
            are served at a public endpoint different from the cluster API server.
            Requires gcloud alpha or beta and --enable-workload-identity.
            """),
      )
      # Keep this hidden as it is not used for user-facing workflows and is
      # eliminated in beta.
      if cls.ReleaseTrack() is base.ReleaseTrack.ALPHA:
        workload_identity_mutex.add_argument(
            '--manage-workload-identity-bucket',
            hidden=True,
            action='store_true',
            help=textwrap.dedent("""\
              Create the GCS bucket for serving OIDC discovery information when
              registering the cluster with Hub. The cluster must already be
              configured with an issuer URL of the format:
              https://storage.googleapis.com/gke-issuer-{UUID}. The cluster must
              also serve the built-in OIDC discovery endpoints by enabling and
              correctly configuring the ServiceAccountIssuerDiscovery feature.
              Requires gcloud alpha and --enable-workload-identity.
              Mutually exclusive with --public-issuer-url.
              """),
        )
        workload_identity_mutex.add_argument(
            '--has-private-issuer',
            hidden=True,
            action='store_true',
            help=textwrap.dedent("""\
              Set to true for clusters where no publicly-routable OIDC discovery
              endpoint for the Kubernetes service account token issuer exists.

              When set to true, the gcloud command-line tool will read the
              private issuer URL and JSON Web Key Set (JWKS) (public keys) for
              validating service account tokens from the cluster's API server
              and upload both when creating the Membership. GCP will then use
              the JWKS, instead of a public OIDC endpoint, to validate service
              account tokens issued by this cluster. Note the JWKS establishes
              the uniqueness of issuers in this configuration, but issuer claims
              in tokens are still compared to the issuer URL associated with the
              Membership when validating tokens.

              Note the cluster's OIDC discovery endpoints
              (https://[KUBE-API-ADDRESS]/.well-known/openid-configuration and
              https://[KUBE-API-ADDRESS]/openid/v1/jwks) must still be
              network-accessible to the gcloud client running this command.
              """),
        )

  def Run(self, args):
    project = arg_utils.GetFromNamespace(args, '--project', use_defaults=True)
    # This incidentally verifies that the kubeconfig and context args are valid.
    with kube_util.KubernetesClient(args) as kube_client:
      kube_client.CheckClusterAdminPermissions()
      kube_util.ValidateClusterIdentifierFlags(kube_client, args)
      uuid = kube_util.GetClusterUUID(kube_client)
      # Read the service account files provided in the arguments early, in order
      # to catch invalid files before performing mutating operations.
      # Service Account key file is required if Workload Identity is not
      # enabled.
      # If Workload Identity is enabled, then the Connect Agent uses
      # a Kubernetes Service Account token instead and hence a GCP Service
      # Account key is not required.
      service_account_key_data = ''
      if args.service_account_key_file:
        try:
          service_account_key_data = hub_util.Base64EncodedFileContents(
              args.service_account_key_file)
        except files.Error as e:
          raise exceptions.Error('Could not process {}: {}'.format(
              SERVICE_ACCOUNT_KEY_FILE_FLAG, e))

      docker_credential_data = None
      if args.docker_credential_file:
        try:
          docker_credential_data = hub_util.Base64EncodedFileContents(
              args.docker_credential_file)
        except files.Error as e:
          raise exceptions.Error('Could not process {}: {}'.format(
              DOCKER_CREDENTIAL_FILE_FLAG, e))

      gke_cluster_self_link = kube_client.processor.gke_cluster_self_link
      issuer_url = None
      private_keyset_json = None
      # enable_workload_identity, public_issuer_url, and
      # manage_workload_identity_bucket are only properties if we are on the
      # alpha or beta track
      if (self.ReleaseTrack() is not base.ReleaseTrack.GA
          and args.enable_workload_identity):
        # public_issuer_url can be None or given by user or gke_cluster_uri
        # (incase of a gke cluster).
        # args.public_issuer_url takes precedence over gke_cluster_uri.
        public_issuer_url = args.public_issuer_url or kube_client.processor.gke_cluster_uri or None

        try:
          openid_config_json = kube_client.GetOpenIDConfiguration(
              issuer_url=public_issuer_url)
        except Exception as e:  # pylint: disable=broad-except
          raise exceptions.Error(
              'Error getting the OpenID Provider Configuration: '
              '{}'.format(e))

        # Extract the issuer URL from the discovery doc.
        issuer_url = json.loads(openid_config_json).get('issuer')
        if not issuer_url:
          raise exceptions.Error('Invalid OpenID Config: '
                                 'missing issuer: {}'.format(
                                     openid_config_json))
        # Ensure public_issuer_url (only non-empty) matches what came back in
        # the discovery doc.
        if public_issuer_url and (public_issuer_url != issuer_url):
          raise exceptions.Error('--public-issuer-url {} did not match issuer '
                                 'returned in discovery doc: {}'.format(
                                     public_issuer_url, issuer_url))

        # Request the JWKS from the cluster if we need it (either for setting
        # up the GCS bucket or getting public keys for private issuers). In
        # the private issuer case, we set private_keyset_json, which is used
        # later to upload the JWKS in the Hub Membership.
        if self.ReleaseTrack() is base.ReleaseTrack.ALPHA:
          if args.manage_workload_identity_bucket:
            api_util.CreateWorkloadIdentityBucket(project, issuer_url,
                                                  openid_config_json,
                                                  kube_client.GetOpenIDKeyset())
          elif args.has_private_issuer:
            private_keyset_json = kube_client.GetOpenIDKeyset()

      # Attempt to create a membership.
      already_exists = False

      obj = None
      # For backward compatiblity, check if a membership was previously created
      # using the cluster uuid.
      parent = api_util.ParentRef(project, 'global')
      membership_id = uuid
      resource_name = api_util.MembershipRef(project, 'global', uuid)
      obj = self._CheckMembershipWithUUID(resource_name, args.CLUSTER_NAME)
      if obj:
        # The membership exists and has the same description.
        already_exists = True
      else:
        # Attempt to create a new membership using cluster_name.
        membership_id = args.CLUSTER_NAME
        resource_name = api_util.MembershipRef(project, 'global',
                                               args.CLUSTER_NAME)
        try:
          self._VerifyClusterExclusivity(kube_client, parent, membership_id)
          obj = api_util.CreateMembership(project, args.CLUSTER_NAME,
                                          args.CLUSTER_NAME,
                                          gke_cluster_self_link, uuid,
                                          self.ReleaseTrack(),
                                          issuer_url, private_keyset_json)
        except apitools_exceptions.HttpConflictError as e:
          # If the error is not due to the object already existing, re-raise.
          error = core_api_exceptions.HttpErrorPayload(e)
          if error.status_description != 'ALREADY_EXISTS':
            raise
          obj = api_util.GetMembership(resource_name, self.ReleaseTrack())
          if not obj.externalId:
            raise exceptions.Error(
                'invalid membership {0} does not have '
                'external_id field set. We cannot determine '
                'if registration is requested against a '
                'valid existing Membership. Consult the '
                'documentation on container hub memberships '
                'update for more information or run gcloud '
                'container hub memberships delete {0} if you '
                'are sure that this is an invalid or '
                'otherwise stale Membership'.format(membership_id))
          if obj.externalId != uuid:
            raise exceptions.Error(
                'membership {0} already exists in the project'
                ' with another cluster. If this operation is'
                ' intended, please run `gcloud container '
                'hub memberships delete {0}` and register '
                'again.'.format(membership_id))

          # The membership exists with same cluster_name.
          already_exists = True

      # In case of an existing membership, check with the user to upgrade the
      # Connect-Agent.
      if already_exists:
        # Update Membership when required. Scenarios that require updates:
        # 1. membership.authority is set, but there is now no issuer URL.
        #    This means the user is disabling Workload Identity.
        # 2. membership.authority is not set, but there is now an
        #    issuer URL. This means the user is enabling Workload Identity.
        # 3. membership.authority is set, but the issuer URL is different
        #    from that set in membership.authority.issuer. This is technically
        #    an error, but we defer to validation in the API.
        # 4. membership.authority.oidcJwks is set, but the private keyset
        #    we got from the cluster differs from the keyset in the membership.
        #    This means the user is updating the public keys, and we should
        #    update to the latest keyset in the membership.
        if self.ReleaseTrack() is not base.ReleaseTrack.GA and (
            # scenario 1, disabling WI
            (obj.authority and not issuer_url) or
            # scenario 2, enabling WI
            (issuer_url and not obj.authority) or
            (obj.authority and
             # scenario 3, issuer changed
             ((obj.authority.issuer != issuer_url) or
              # scenario 4, JWKS changed
              (private_keyset_json and obj.authority.oidcJwks and
               (obj.authority.oidcJwks.decode('utf-8') !=
                private_keyset_json))))):
          console_io.PromptContinue(
              message=hub_util.GenerateWIUpdateMsgString(
                  obj, issuer_url, resource_name, args.CLUSTER_NAME),
              cancel_on_no=True)
          try:
            api_util.UpdateMembership(resource_name, obj, 'authority',
                                      self.ReleaseTrack(),
                                      issuer_url=issuer_url,
                                      oidc_jwks=private_keyset_json)
            log.status.Print(
                'Updated the membership [{}] for the cluster [{}]'.format(
                    resource_name, args.CLUSTER_NAME))
          except Exception as e:
            raise exceptions.Error(
                'Error in updating the membership [{}]:{}'.format(
                    resource_name, e))
        else:
          console_io.PromptContinue(
              message='A membership [{}] for the cluster [{}] already exists. '
              'Continuing will reinstall the Connect agent deployment to use a '
              'new image (if one is available).'.format(resource_name,
                                                        args.CLUSTER_NAME),
              cancel_on_no=True)
      else:
        log.status.Print(
            'Created a new membership [{}] for the cluster [{}]'.format(
                resource_name, args.CLUSTER_NAME))

      # Attempt to update the existing agent deployment, or install a new agent
      # if necessary.
      try:
        self._InstallOrUpdateExclusivityArtifacts(kube_client, resource_name)
        agent_util.DeployConnectAgent(kube_client, args,
                                      service_account_key_data,
                                      docker_credential_data, resource_name,
                                      self.ReleaseTrack())
      except Exception as e:
        log.status.Print('Error in installing the Connect Agent: {}'.format(e))
        # In case of a new membership, we need to clean up membership and
        # resources if we failed to install the Connect Agent.
        if not already_exists:
          api_util.DeleteMembership(resource_name, self.ReleaseTrack())
          exclusivity_util.DeleteMembershipResources(kube_client)
        raise
      log.status.Print(
          'Finished registering the cluster [{}] with the Hub.'.format(
              args.CLUSTER_NAME))
      return obj

  def _CheckMembershipWithUUID(self, resource_name, cluster_name):
    """Checks for an existing Membership with UUID.

    In the past, by default we used Cluster UUID to create a membership. Now
    we use user supplied cluster_name. This check ensures that we don't
    reregister a cluster.

    Args:
      resource_name: The full membership resource name using the cluster uuid.
      cluster_name: User supplied cluster_name.

    Returns:
     The Membership resource or None.

    Raises:
      exceptions.Error: If it fails to getMembership.
    """
    obj = None
    try:
      obj = api_util.GetMembership(resource_name, self.ReleaseTrack())
      if (hasattr(obj, 'description') and obj.description != cluster_name):
        # A membership exists, but does not have the same cluster_name.
        # This is possible if two different users attempt to register the same
        # cluster, or if the user is upgrading and has passed a different
        # cluster_name. Treat this as an error: even in the upgrade case,
        # this is useful to prevent the user from upgrading the wrong cluster.
        raise exceptions.Error(
            'There is an existing membership, [{}], that conflicts with [{}]. '
            'Please delete it before continuing:\n\n'
            '  gcloud {}container hub memberships delete {}'.format(
                obj.description, cluster_name,
                hub_util.ReleaseTrackCommandPrefix(self.ReleaseTrack()),
                resource_name))
    except apitools_exceptions.HttpNotFoundError:
      # We couldn't find a membership with uuid, so it's safe to create a
      # new one.
      pass
    return obj

  def _VerifyClusterExclusivity(self, kube_client, parent, membership_id):
    """Verifies that the cluster can be registered to the project.

    Args:
      kube_client: a KubernetesClient
      parent: the parent collection the user is attempting to register the
        cluster with.
      membership_id: the ID of the membership to be created for the cluster.

    Raises:
      apitools.base.py.HttpError: if the API request returns an HTTP error.
      exceptions.Error: if the cluster is in an invalid exclusivity state.
    """

    cr_manifest = ''
    # The cluster has been registered.
    if kube_client.MembershipCRDExists():
      cr_manifest = kube_client.GetMembershipCR()

    res = api_util.ValidateExclusivity(cr_manifest, parent,
                                       membership_id,
                                       self.ReleaseTrack())

    if res.status.code:
      raise exceptions.Error(
          'Error validating cluster\'s exclusivity state '
          'with the Hub under parent collection [{}]: {}. '
          'Cannot proceed with the cluster registration.'.format(
              parent, res.status.message))

  def _InstallOrUpdateExclusivityArtifacts(self, kube_client, membership_ref):
    """Install the exclusivity artifacts for the cluster.

    Update the exclusivity artifacts if a new version is available if the
    cluster has already being registered.

    Args:
      kube_client: a KubernetesClient
      membership_ref: the full resource name of the membership the cluster is
        registered with.

    Raises:
      apitools.base.py.HttpError: if the API request returns an HTTP error.
      exceptions.Error: if the kubectl interation with the cluster failed.
    """
    crd_manifest = kube_client.GetMembershipCRD()
    cr_manifest = kube_client.GetMembershipCR() if crd_manifest else ''
    res = api_util.GenerateExclusivityManifest(crd_manifest,
                                               cr_manifest,
                                               membership_ref)
    kube_client.ApplyMembership(res.crdManifest, res.crManifest)
