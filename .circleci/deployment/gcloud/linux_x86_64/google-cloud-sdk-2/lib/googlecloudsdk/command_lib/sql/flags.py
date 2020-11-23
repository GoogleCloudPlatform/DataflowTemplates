# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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
"""Common flags for some of the SQL commands.

Flags are specified with functions that take in a single argument, the parser,
and add the newly constructed flag to that parser.

Example:

def AddFlagName(parser):
  parser.add_argument(
    '--flag-name',
    ... // Other flag details.
  )
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import utils as compute_utils
from googlecloudsdk.api_lib.storage import storage_util
from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.calliope import actions
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.util import completers

messages = apis.GetMessagesModule('sql', 'v1beta4')

_IP_ADDRESS_PART = r'(25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})'  # Match decimal 0-255
_CIDR_PREFIX_PART = r'([0-9]|[1-2][0-9]|3[0-2])'  # Match decimal 0-32
# Matches either IPv4 range in CIDR notation or a naked IPv4 address.
_CIDR_REGEX = r'{addr_part}(\.{addr_part}){{3}}(\/{prefix_part})?$'.format(
    addr_part=_IP_ADDRESS_PART, prefix_part=_CIDR_PREFIX_PART)


class DatabaseCompleter(completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(DatabaseCompleter, self).__init__(
        collection='sql.databases',
        api_version='v1beta4',
        list_command='sql databases list --uri',
        flags=['instance'],
        **kwargs)


class InstanceCompleter(completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(InstanceCompleter, self).__init__(
        collection='sql.instances',
        list_command='sql instances list --uri',
        **kwargs)


class UserCompleter(completers.ListCommandCompleter):

  def __init__(self, **kwargs):
    super(UserCompleter, self).__init__(
        collection=None,  # TODO(b/62961917): Should be 'sql.users',
        api_version='v1beta4',
        list_command='sql users list --flatten=name[] --format=disable',
        flags=['instance'],
        **kwargs)


def AddInstance(parser):
  parser.add_argument(
      '--instance',
      '-i',
      required=True,
      completer=InstanceCompleter,
      help='Cloud SQL instance ID.')


def AddInstanceArgument(parser):
  """Add the 'instance' argument to the parser."""
  parser.add_argument(
      'instance', completer=InstanceCompleter, help='Cloud SQL instance ID.')


# Currently, 10230 is the max storage size one can set, and 10 is the minimum.
def AddInstanceResizeLimit(parser):
  parser.add_argument(
      '--storage-auto-increase-limit',
      type=arg_parsers.BoundedInt(10, 10230, unlimited=True),
      help='Allows you to set a maximum storage capacity, in GB. Automatic '
      'increases to your capacity will stop once this limit has been reached. '
      'Default capacity is *unlimited*.')


def AddUsername(parser):
  parser.add_argument(
      'username', completer=UserCompleter, help='Cloud SQL username.')


def AddHost(parser):
  """Add the '--host' flag to the parser."""
  parser.add_argument(
      '--host',
      help=('Cloud SQL user\'s host name expressed as a specific IP address'
            ' or address range. `%` denotes an unrestricted host name. '
            'Applicable flag for MySQL instances; ignored for all other '
            'engines. Note, if you connect to your instance using IP '
            'addresses, you must add your client IP address as an Authorized'
            ' Address, even if your host name is unrestricted. For help on '
            'how to do so, read: '
            'https://cloud.google.com/sql/docs/mysql/configure-ip'))


def AddAvailabilityType(parser):
  """Add the '--availability-type' flag to the parser."""
  availabilty_type_flag = base.ChoiceArgument(
      '--availability-type',
      required=False,
      choices={
          'regional': 'Provides high availability and is recommended for '
                      'production instances; instance automatically fails over '
                      'to another zone within your selected region.',
          'zonal': 'Provides no failover capability. This is the default.'
      },
      help_str=('Specifies level of availability.'))
  availabilty_type_flag.AddToParser(parser)


def AddPassword(parser):
  parser.add_argument('--password', help='Cloud SQL user\'s password.')


def AddRootPassword(parser):
  """Add the root password field to the parser."""
  parser.add_argument(
      '--root-password',
      required=False,
      help='Root Cloud SQL user\'s password.')


def AddPromptForPassword(parser):
  parser.add_argument(
      '--prompt-for-password',
      action='store_true',
      help=('Prompt for the Cloud SQL user\'s password with character echo '
            'disabled. The password is all typed characters up to but not '
            'including the RETURN or ENTER key.'))


def AddType(parser):
  parser.add_argument('--type', help='Cloud SQL user\'s type. It determines '
                      'the method to authenticate the user during login. '
                      'See the list of user types at '
                      'https://cloud.google.com/sql/docs/postgres/admin-api/'
                      'v1beta4/users#type')


# Instance create and patch flags


def AddActivationPolicy(parser):
  base.ChoiceArgument(
      '--activation-policy',
      required=False,
      choices=['always', 'never', 'on-demand'],
      default=None,
      help_str=('Activation policy for this instance. This specifies when '
                'the instance should be activated and is applicable only when '
                'the instance state is `RUNNABLE`. The default is `on-demand`. '
                'More information on activation policies can be found here: '
                'https://cloud.google.com/sql/faq#activation_policy'
               )).AddToParser(parser)


def AddAssignIp(parser):
  parser.add_argument(
      '--assign-ip',
      help='Assign a public IP address to the instance. This is a public, '
      'externally available IPv4 address that you can use to connect to your '
      'instance when properly authorized. Use --assign-ip to enable a public '
      'IP and --no-assign-ip to disable it.',
      action=arg_parsers.StoreTrueFalseAction)


def AddAuthorizedGAEApps(parser, update=False):
  help_ = (
      'First Generation instances only. List of project IDs for App Engine '
      'applications running in the Standard environment that '
      'can access this instance.')
  if update:
    help_ += (
        '\n\nThe value given for this argument *replaces* the existing list.')
  parser.add_argument(
      '--authorized-gae-apps',
      type=arg_parsers.ArgList(min_length=1),
      metavar='APP',
      required=False,
      help=help_)


def AddAuthorizedNetworks(parser, update=False):
  """Adds the `--authorized-networks` flag."""
  cidr_validator = arg_parsers.RegexpValidator(
      _CIDR_REGEX, ('Must be specified in CIDR notation, also known as '
                    '\'slash\' notation (e.g. 192.168.100.0/24).'))
  help_ = ('The list of external networks that are allowed to connect to '
           'the instance. Specified in CIDR notation, also known as '
           '\'slash\' notation (e.g. 192.168.100.0/24).')
  if update:
    help_ += (
        '\n\nThe value given for this argument *replaces* the existing list.')
  parser.add_argument(
      '--authorized-networks',
      type=arg_parsers.ArgList(min_length=1, element_type=cidr_validator),
      metavar='NETWORK',
      required=False,
      default=[],
      help=help_)


def AddBackupStartTime(parser):
  parser.add_argument(
      '--backup-start-time',
      required=False,
      help=('Start time of daily backups, specified in the 24 hour '
            'format - HH:MM, in the UTC timezone.'))


def AddBackupLocation(parser, allow_empty):
  help_text = (
      'Choose where to store your backups. Backups are stored in the closest '
      'multi-region location to you by default. Only customize if needed.')
  if allow_empty:
    help_text += ' Specify empty string to revert to default.'
  parser.add_argument('--backup-location', required=False, help=help_text)


# Currently, MAX_BACKUP_RETENTION_COUNT=365, and MIN_BACKUP_RETENTION_COUNT=1.
def AddRetainedBackupsCount(parser):
  help_text = (
      'How many backups to keep. The valid range is between 1 and 365. The '
      'default value is 7 if not specified. Applicable only if --no-backups is '
      'not specified.')
  parser.add_argument(
      '--retained-backups-count',
      type=arg_parsers.BoundedInt(1, 365, unlimited=False),
      help=help_text)


# Currently, MAX_TRANSACTION_LOG_RETENTION_DAYS=7, and
# MIN_TRANSACTION_LOG_RETENTION_DAYS=1.
def AddRetainedTransactionLogDays(parser):
  help_text = (
      'How many days of transaction logs to keep. The valid range is between '
      '1 and 7. The default value is 7 if not specified. Only valid when '
      'point in time recovery is enabled. Keeping more days of transaction '
      'logs requires bigger storage size')
  parser.add_argument(
      '--retained-transaction-log-days',
      type=arg_parsers.BoundedInt(1, 7, unlimited=False),
      help=help_text)


def AddDatabaseFlags(parser, update=False):
  """Adds the `--database-flags` flag."""
  help_ = ('Comma-separated list of database flags to set on the '
           'instance. Use an equals sign to separate flag name and value. '
           'Flags without values, like skip_grant_tables, can be written '
           'out without a value after, e.g., `skip_grant_tables=`. Use '
           'on/off for booleans. View the Instance Resource API for allowed '
           'flags. (e.g., `--database-flags max_allowed_packet=55555,'
           'skip_grant_tables=,log_output=1`)')
  if update:
    help_ += (
        '\n\nThe value given for this argument *replaces* the existing list.')
  parser.add_argument(
      '--database-flags',
      type=arg_parsers.ArgDict(min_length=1),
      metavar='FLAG=VALUE',
      required=False,
      help=help_)


def AddDatabaseVersion(parser, restrict_choices=True):
  """Adds `--database-version` to the parser with choices restricted or not."""
  # Section for engine-specific content.
  # This section is auto-generated by //cloud/storage_fe/sql/sync_engines.
  # Do not make manual edits.
  choices = [
      'MYSQL_5_6',
      'MYSQL_5_7',
      'MYSQL_8_0',
      'POSTGRES_9_6',
      'POSTGRES_10',
      'POSTGRES_11',
      'POSTGRES_12',
      'POSTGRES_13',
      'SQLSERVER_2017_EXPRESS',
      'SQLSERVER_2017_WEB',
      'SQLSERVER_2017_STANDARD',
      'SQLSERVER_2017_ENTERPRISE',
  ]
  # End of engine-specific content.
  help_text = (
      'The database engine type and version. If left unspecified, the API '
      'defaults will be used.')
  if not restrict_choices:
    help_text = ' '.join([
        help_text, 'See the list of database versions at '
        'https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances'
        '#databaseVersion'
    ])
  parser.add_argument(
      '--database-version',
      required=False,
      choices=choices if restrict_choices else None,
      help=help_text)


def AddCPU(parser):
  parser.add_argument(
      '--cpu',
      type=int,
      required=False,
      help=('Whole number value indicating how many cores are desired in '
            'the machine. Both --cpu and --memory must be specified if a '
            'custom machine type is desired, and the --tier flag must be '
            'omitted.'))


def _GetKwargsForBoolFlag(show_negated_in_help):
  if show_negated_in_help:
    return {
        'action': arg_parsers.StoreTrueFalseAction,
    }
  else:
    return {'action': 'store_true', 'default': None}


def AddInstanceCollation(parser):
  parser.add_argument(
      '--collation',
      help='Cloud SQL server-level collation setting, which specifies '
      'the set of rules for comparing characters in a character set.')


def AddEnableBinLog(parser, show_negated_in_help=False):
  kwargs = _GetKwargsForBoolFlag(show_negated_in_help)
  parser.add_argument(
      '--enable-bin-log',
      required=False,
      help=('Allows for data recovery from a specific point in time, down to a '
            'fraction of a second. Must have automatic backups enabled to use. '
            'Make sure storage can support at least 7 days of logs.'),
      **kwargs)


def AddEnablePointInTimeRecovery(parser, show_negated_in_help=False):
  kwargs = _GetKwargsForBoolFlag(show_negated_in_help)
  parser.add_argument(
      '--enable-point-in-time-recovery',
      required=False,
      help=('Allows for data recovery from a specific point in time, down to a '
            'fraction of a second, via write-ahead logs. Must have automatic '
            'backups enabled to use. Make sure storage can support at least 7 '
            'days of logs.'),
      **kwargs)


def AddExternalMasterGroup(parser):
  """Add flags to the parser for creating an external master and replica."""

  # Group for creating external primary instances.
  external_master_group = parser.add_group(
      required=False,
      help='Options for creating a wrapper for an external data source.')
  external_master_group.add_argument(
      '--source-ip-address',
      required=True,
      type=compute_utils.IPV4Argument,
      help=('Public IP address used to connect to and replicate from '
            'the external data source.'))
  external_master_group.add_argument(
      '--source-port',
      type=arg_parsers.BoundedInt(lower_bound=1, upper_bound=65535),
      # Default MySQL port number.
      default=3306,
      help=('Port number used to connect to and replicate from the '
            'external data source.'))

  # Group for creating replicas of external primary instances.
  internal_replica_group = parser.add_group(
      required=False,
      help=('Options for creating an internal replica of an external data '
            'source.'))
  internal_replica_group.add_argument(
      '--master-username',
      required=True,
      help='Name of the replication user on the external data source.')

  # TODO(b/78648703): Make group required when mutex required status is fixed.
  # For entering the password of the replication user of an external primary.
  master_password_group = internal_replica_group.add_group(
      'Password group.', mutex=True)
  master_password_group.add_argument(
      '--master-password',
      help='Password of the replication user on the external data source.')
  master_password_group.add_argument(
      '--prompt-for-master-password',
      action='store_true',
      help=('Prompt for the password of the replication user on the '
            'external data source. The password is all typed characters up '
            'to but not including the RETURN or ENTER key.'))
  internal_replica_group.add_argument(
      '--master-dump-file-path',
      required=True,
      type=storage_util.ObjectReference.FromArgument,
      help=('Path to the MySQL dump file in Google Cloud Storage from '
            'which the seed import is made. The URI is in the form '
            'gs://bucketName/fileName. Compressed gzip files (.gz) are '
            'also supported.'))

  # For specifying SSL certs for connecting to an external primary.
  credential_group = internal_replica_group.add_group(
      'Client and server credentials.', required=False)
  credential_group.add_argument(
      '--master-ca-certificate-path',
      required=True,
      help=('Path to a file containing the X.509v3 (RFC5280) PEM encoded '
            'certificate of the CA that signed the external data source\'s '
            'certificate.'))

  # For specifying client certs for connecting to an external primary.
  client_credential_group = credential_group.add_group(
      'Client credentials.', required=False)
  client_credential_group.add_argument(
      '--client-certificate-path',
      required=True,
      help=('Path to a file containing the X.509v3 (RFC5280) PEM encoded '
            'certificate that will be used by the replica to authenticate '
            'against the external data source.'))
  client_credential_group.add_argument(
      '--client-key-path',
      required=True,
      help=('Path to a file containing the unencrypted PKCS#1 or PKCS#8 '
            'PEM encoded private key associated with the '
            'clientCertificate.'))


def AddFollowGAEApp(parser):
  parser.add_argument(
      '--follow-gae-app',
      required=False,
      help=('First Generation instances only. The App Engine app '
            'this instance should follow. It must be in the same region as '
            'the instance. WARNING: Instance may be restarted.'))


def AddMaintenanceReleaseChannel(parser):
  base.ChoiceArgument(
      '--maintenance-release-channel',
      choices={
          'production': 'Production updates are stable and recommended '
                        'for applications in production.',
          'preview': 'Preview updates release prior to production '
                     'updates. You may wish to use the preview channel '
                     'for dev/test applications so that you can preview '
                     'their compatibility with your application prior '
                     'to the production release.'
      },
      help_str=("Which channel's updates to apply during the maintenance "
                'window. If not specified, Cloud SQL chooses the timing of '
                'updates to your instance.')).AddToParser(parser)


def AddMaintenanceWindowDay(parser):
  parser.add_argument(
      '--maintenance-window-day',
      choices=arg_parsers.DayOfWeek.DAYS,
      type=arg_parsers.DayOfWeek.Parse,
      help='Day of week for maintenance window, in UTC time zone.')


def AddMaintenanceWindowHour(parser):
  parser.add_argument(
      '--maintenance-window-hour',
      type=arg_parsers.BoundedInt(lower_bound=0, upper_bound=23),
      help='Hour of day for maintenance window, in UTC time zone.')


def AddDenyMaintenancePeriodStartDate(parser):
  parser.add_argument(
      '--deny-maintenance-period-start-date',
      help='''Date when the deny maintenance period begins,
       that is ``2020-11-01".''')


def AddDenyMaintenancePeriodEndDate(parser):
  parser.add_argument(
      '--deny-maintenance-period-end-date',
      help='Date when the deny maintenance period ends, that is ``2021-01-10".')


def AddDenyMaintenancePeriodTime(parser):
  parser.add_argument(
      '--deny-maintenance-period-time',
      help='''Time when the deny maintenance period starts or ends,
       that is ``05:00:00".''')


def AddInsightsConfigQueryInsightsEnabled(parser, show_negated_in_help=False):
  kwargs = _GetKwargsForBoolFlag(show_negated_in_help)
  parser.add_argument(
      '--insights-config-query-insights-enabled',
      required=False,
      help="""Enable query insights feature to provide query and query plan
        analytics. To disable this feature, use
        `--no-insights-config-query-insights-enabled` instead.""",
      **kwargs)


def AddInsightsConfigQueryStringLength(parser):
  parser.add_argument(
      '--insights-config-query-string-length',
      required=False,
      type=arg_parsers.BoundedInt(lower_bound=256, upper_bound=4500),
      help="""Query string length in bytes to be stored by the query insights
        feature. Default length is 1024 bytes. Allowed range: 256 to 4500
        bytes.""")


def AddInsightsConfigRecordApplicationTags(parser, show_negated_in_help=False):
  kwargs = _GetKwargsForBoolFlag(show_negated_in_help)
  parser.add_argument(
      '--insights-config-record-application-tags',
      required=False,
      help="""Allow application tags to be recorded by the query insights
        feature. To disable application tag recording, use
        `--no-insights-record-application-tags` flag instead.""",
      **kwargs)


def AddInsightsConfigRecordClientAddress(parser, show_negated_in_help=False):
  kwargs = _GetKwargsForBoolFlag(show_negated_in_help)
  parser.add_argument(
      '--insights-config-record-client-address',
      required=False,
      help="""Allow the client address to be recorded by the query insights
        feature. To disable client address recording, use
        `--no-insights-config-record-client-address` instead.""",
      **kwargs)


def AddMemory(parser):
  parser.add_argument(
      '--memory',
      type=arg_parsers.BinarySize(),
      required=False,
      help=('Whole number value indicating how much memory is desired in '
            'the machine. A size unit should be provided (eg. 3072MiB or '
            '9GiB) - if no units are specified, GiB is assumed. Both --cpu '
            'and --memory must be specified if a custom machine type is '
            'desired, and the --tier flag must be omitted.'))


def AddNetwork(parser):
  """Adds the `--network` flag to the parser."""
  parser.add_argument(
      '--network',
      help=('Network in the current project that the instance will be part '
            'of. To specify using a network with a shared VPC, use the full '
            'URL of the network. For an example host project, \'testproject\', '
            'and shared network, \'testsharednetwork\', this would be of the '
            'form:'
            '`--network`=`projects/testproject/global/networks/'
            'testsharednetwork`'))


def AddReplication(parser):
  base.ChoiceArgument(
      '--replication',
      required=False,
      choices=['synchronous', 'asynchronous'],
      default=None,
      help_str='Type of replication this instance uses. The default is '
      'synchronous.').AddToParser(parser)


def AddStorageAutoIncrease(parser):
  parser.add_argument(
      '--storage-auto-increase',
      action='store_true',
      default=None,
      help=('Storage size can be increased, but it cannot be decreased; '
            'storage increases are permanent for the life of the instance. '
            'With this setting enabled, a spike in storage requirements '
            'can result in permanently increased storage costs for your '
            'instance. However, if an instance runs out of available space, '
            'it can result in the instance going offline, dropping existing '
            'connections. This setting is enabled by default.'))


def AddStorageSize(parser):
  parser.add_argument(
      '--storage-size',
      type=arg_parsers.BinarySize(
          lower_bound='10GB',
          upper_bound='30720GB',
          suggested_binary_size_scales=['GB']),
      help=('Amount of storage allocated to the instance. Must be an integer '
            'number of GB. The default is 10GB. Information on storage '
            'limits can be found here: '
            'https://cloud.google.com/sql/docs/quotas#storage_limits'))


def AddTier(parser, is_patch=False):
  """Adds the '--tier' flag to the parser."""
  help_text = ('The tier for this instance. For Second Generation instances, '
               'TIER is the instance\'s machine type (e.g., db-n1-standard-1). '
               'For PostgreSQL instances, only shared-core machine types '
               '(e.g., db-f1-micro) apply. A complete list of tiers is '
               'available here: https://cloud.google.com/sql/pricing.')
  if is_patch:
    help_text += ' WARNING: Instance will be restarted.'

  parser.add_argument('--tier', '-t', required=False, help=help_text)


def AddZone(parser, help_text):
  """Adds the mutually exclusive `--gce-zone` and `--zone` to the parser."""
  zone_group = parser.add_mutually_exclusive_group()
  zone_group.add_argument(
      '--gce-zone',
      required=False,
      action=actions.DeprecationAction(
          '--gce-zone',
          removed=False,
          warn=('Flag `{flag_name}` is deprecated and will be removed by '
                'release 255.0.0. Use `--zone` instead.')),
      help=help_text)
  zone_group.add_argument('--zone', required=False, help=help_text)


# Database specific flags


def AddDatabaseName(parser):
  parser.add_argument(
      'database', completer=DatabaseCompleter, help='Cloud SQL database name.')


def AddCharset(parser):
  parser.add_argument(
      '--charset',
      help='Cloud SQL database charset setting, which specifies the '
      'set of symbols and encodings used to store the data in your database. '
      'Each database version may support a different set of charsets.')


def AddCollation(parser):
  parser.add_argument(
      '--collation',
      help='Cloud SQL database collation setting, which specifies '
      'the set of rules for comparing characters in a character set. Each'
      ' database version may support a different set of collations. For '
      'PostgreSQL database versions, this may only be set to the collation of '
      'the template database.')


def AddOperationArgument(parser):
  parser.add_argument(
      'operation',
      nargs='+',
      help='An identifier that uniquely identifies the operation.')


# Instance export / import flags.


def AddUriArgument(parser, help_text):
  """Add the 'uri' argument to the parser, with help text help_text."""
  parser.add_argument('uri', help=help_text)


def AddOffloadArgument(parser):
  """Add the 'offload' argument to the parser."""
  parser.add_argument(
      '--offload',
      action='store_true',
      help=(
          'Offload an export to a temporary instance. Doing so reduces strain '
          'on source instances and allows other operations to be performed '
          'while the export is in progress.'
      ))


DEFAULT_DATABASE_IMPORT_HELP_TEXT = (
    'Database to which the import is made. If not set, it is assumed that '
    'the database is specified in the file to be imported. If your SQL '
    'dump file includes a database statement, it will override the '
    'database set in this flag.')

SQLSERVER_DATABASE_IMPORT_HELP_TEXT = (
    'A new database into which the import is made.')


def AddDatabase(parser, help_text, required=False):
  """Add the '--database' and '-d' flags to the parser.

  Args:
    parser: The current argparse parser to add these database flags to.
    help_text: String, specifies the help text for the database flags.
    required: Boolean, specifies whether the database flag is required.
  """
  parser.add_argument('--database', '-d', required=required, help=help_text)


DEFAULT_DATABASE_LIST_EXPORT_HELP_TEXT = (
    'Database(s) from which the export is made. Information on requirements '
    'can be found here: https://cloud.google.com/sql/docs/mysql/admin-api/'
    'v1beta4/instances/export#exportContext.databases')

SQLSERVER_DATABASE_LIST_EXPORT_HELP_TEXT = (
    'Database from which the export is made. Information on requirements '
    'can be found here: https://cloud.google.com/sql/docs/sqlserver/admin-api/'
    'v1beta4/instances/export#exportContext.databases')


def AddDatabaseList(parser, help_text, required=False):
  """Add the '--database' and '-d' list flags to the parser.

  Args:
    parser: The current argparse parser to add these database flags to.
    help_text: String, specifies the help text for the database flags.
    required: Boolean, specifies whether the database flag is required.
  """
  if required:
    group = parser.add_group(mutex=False, required=True)
    group.add_argument(
        '--database',
        '-d',
        type=arg_parsers.ArgList(min_length=1),
        metavar='DATABASE',
        help=help_text)
  else:
    parser.add_argument(
        '--database',
        '-d',
        type=arg_parsers.ArgList(min_length=1),
        metavar='DATABASE',
        required=False,
        help=help_text)


def AddUser(parser, help_text):
  """Add the '--user' flag to the parser, with help text help_text."""
  parser.add_argument('--user', help=help_text)


def AddEncryptedBakFlags(parser):
  """Add the flags for importing encrypted BAK files.

  Add the --cert-path, --pvk-path, --pvk-password and
  --prompt-for-pvk-password flags to the parser

  Args:
    parser: The current argparse parser to add these database flags to.
  """
  enc_group = parser.add_group(
      mutex=False,
      required=False,
      help='Encryption info to support importing an encrypted .bak file')
  enc_group.add_argument(
      '--cert-path',
      required=True,
      help=('Path to the encryption certificate file in Google Cloud Storage '
            'associated with the BAK file. The URI is in the form '
            '`gs://bucketName/fileName`.'))
  enc_group.add_argument(
      '--pvk-path',
      required=True,
      help=('Path to the encryption private key file in Google Cloud Storage '
            'associated with the BAK file. The URI is in the form '
            '`gs://bucketName/fileName`.'))
  password_group = enc_group.add_group(mutex=True, required=True)
  password_group.add_argument(
      '--pvk-password',
      help='The private key password associated with the BAK file.')
  password_group.add_argument(
      '--prompt-for-pvk-password',
      action='store_true',
      help=(
          'Prompt for the private key password associated with the BAK file '
          'with character echo disabled. The password is all typed characters '
          'up to but not including the RETURN or ENTER key.'))


def AddRescheduleType(parser):
  """Add the flag to specify reschedule type.

  Args:
    parser: The current argparse parser to add this to.
  """
  choices = [
      messages.Reschedule.RescheduleTypeValueValuesEnum.IMMEDIATE.name,
      messages.Reschedule.RescheduleTypeValueValuesEnum.NEXT_AVAILABLE_WINDOW
      .name,
      messages.Reschedule.RescheduleTypeValueValuesEnum.SPECIFIC_TIME.name,
  ]
  help_text = 'The type of reschedule operation to perform.'
  parser.add_argument(
      '--reschedule-type', choices=choices, required=True, help=help_text)


def AddScheduleTime(parser):
  """Add the flag for maintenance reschedule schedule time.

  Args:
    parser: The current argparse parser to add this to.
  """
  parser.add_argument(
      '--schedule-time',
      type=arg_parsers.Datetime.Parse,
      help=('When specifying SPECIFIC_TIME, the date and time at which to '
            'schedule the maintenance in ISO 8601 format.'))


INSTANCES_USERLABELS_FORMAT = ':(settings.userLabels:alias=labels:label=LABELS)'

INSTANCES_FORMAT_COLUMNS = [
    'name', 'databaseVersion', 'firstof(gceZone,region):label=LOCATION',
    'settings.tier',
    'ip_addresses.filter("type:PRIMARY").*extract(ip_address).flatten()'
    '.yesno(no="-"):label=PRIMARY_ADDRESS',
    'ip_addresses.filter("type:PRIVATE").*extract(ip_address).flatten()'
    '.yesno(no="-"):label=PRIVATE_ADDRESS', 'state:label=STATUS'
]


def GetInstanceListFormat():
  """Returns the table format for listing instances."""
  table_format = '{} table({})'.format(INSTANCES_USERLABELS_FORMAT,
                                       ','.join(INSTANCES_FORMAT_COLUMNS))
  return table_format


OPERATION_FORMAT = """
  table(
    operation,
    operationType:label=TYPE,
    startTime.iso():label=START,
    endTime.iso():label=END,
    error.errors[0].code.yesno(no="-"):label=ERROR,
    state:label=STATUS
  )
"""

OPERATION_FORMAT_BETA = """
  table(
    name,
    operationType:label=TYPE,
    startTime.iso():label=START,
    endTime.iso():label=END,
    error.errors[0].code.yesno(no="-"):label=ERROR,
    status:label=STATUS
  )
"""

CLIENT_CERTS_FORMAT = """
  table(
    commonName:label=NAME,
    sha1Fingerprint,
    expirationTime.yesno(no="-"):label=EXPIRATION
  )
"""

SERVER_CA_CERTS_FORMAT = """
  table(
    sha1Fingerprint,
    expirationTime.yesno(no="-"):label=EXPIRATION
  )
"""

TIERS_FORMAT = """
  table(
    tier,
    region.list():label=AVAILABLE_REGIONS,
    RAM.size(),
    DiskQuota.size():label=DISK
  )
"""

USERS_FORMAT = """
  table(
    name.yesno(no='(anonymous)'),
    host
  )
"""

USERS_FORMAT_BETA = """
  table(
    name.yesno(no='(anonymous)'),
    host,
    type.yesno(no='BUILT_IN')
  )
"""


USERS_FORMAT_ALPHA = USERS_FORMAT_BETA


def AddActiveDirectoryDomain(parser):
  """Adds the '--active-directory-domain' flag to the parser.

  Args:
    parser: The current argparse parser to add this to.
  """
  help_text = (
      'Managed Service for Microsoft Active Directory domain this instance is '
      'joined to. Only available for SQL Server instances.'
  )
  parser.add_argument('--active-directory-domain', help=help_text)
