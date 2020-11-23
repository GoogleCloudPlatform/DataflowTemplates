# -*- coding: utf-8 -*- #
# Copyright 2020 Google LLC. All Rights Reserved.
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
"""CDN Flexible cache control flags for the compute backend-services and compute backend-buckets commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import arg_parsers


def AddFlexibleCacheStepOne(parser, resource_name, update_command=False):
  """Adds cache mode, max ttl, default ttl, client ttl, negative caching, negative caching policy and custom response header  args to the argparse."""
  # TODO (b/165456063) document enums as lowercase-with-dash. Accept both forms.
  parser.add_argument(
      '--cache-mode',
      choices={
          'CACHE_ALL_STATIC':
              """Automatically cache static content, including common image
              formats, media (video and audio), web assets (JavaScript and CSS).
              Requests and responses that are marked as uncacheable, as well as
              dynamic content (including HTML), aren't cached.""",
          'USE_ORIGIN_HEADERS':
              """Require the origin to set valid caching headers to cache
              content. Responses without these headers aren't cached at
              Google's edge, and require a full trip to the origin on every
              request, potentially impacting performance and increasing load on
              the origin server.""",
          'FORCE_CACHE_ALL':
              """Cache all content, ignoring any "private", "no-store" or
              "no-cache" directives in Cache-Control response headers. Warning:
              this may result in Cloud CDN caching private, per-user (user
              identifiable) content. You should only enable this on backends
              that are not serving private or dynamic content, such as storage
              buckets."""
      },
      type=lambda x: x.replace('-', '_').upper(),
      default=None,
      help="""\
      Specifies the cache setting for all responses from this backend.
      """)
  client_ttl_help = """\
  Specifies a separate client (for example, browser client) TTL, separate from the TTL
  for Cloud CDN's edge caches.

  This allows you to set a shorter TTL for browsers/clients, and to have those
  clients revalidate content against Cloud CDN on a more regular basis, without
  requiring revalidation at the origin.

  The value of clientTtl cannot be set to a value greater than that of maxTtl,
  but can be equal.

  Any cacheable response has its max-age/s-maxage directives adjusted down to
  the client TTL value if necessary; an Expires header will be replaced with a
  suitable max-age directive.

  The maximum allowed value is 86400s (1 day).

  When creating a new backend with CACHE_ALL_STATIC and the field is unset, or
  when switching to that mode and the field is unset, a default value of 3600
  is used.

  When the cache mode is set to "USE_ORIGIN_HEADERS", you must omit this field.
  """
  client_ttl_group = parser.add_mutually_exclusive_group()
  client_ttl_group.add_argument(
      '--client-ttl',
      type=arg_parsers.Duration(upper_bound=86400),
      default=None,
      help=client_ttl_help,
  )
  if update_command:
    client_ttl_group.add_argument(
        '--no-client-ttl', action='store_true', help='Clears client TTL value.')
  default_ttl_help = """\
  Specifies the default TTL for cached content served by this origin for
  responses that do not have an existing valid TTL (max-age or s-maxage).

  The default value is 3600s for cache modes that allow a default TTL to be
  defined.

  The value of defaultTtl cannot be set to a value greater than that of maxTtl,
  but can be equal.

  When the cacheMode is set to FORCE_CACHE_ALL, the defaultTtl overwrites
  the TTL set in all responses.

  A TTL of "0" means Always revalidate.

  The maximum allowed value is 31,622,400s (1 year). Infrequently
  accessed objects may be evicted from the cache before the defined TTL.

  When creating a new backend with CACHE_ALL_STATIC or FORCE_CACHE_ALL and the
  field is unset, or when updating an existing backend to use these modes and
  the field is unset, a default value of 3600 is used. When the cache mode is
  set to "USE_ORIGIN_HEADERS", you must omit this field.
  """
  default_ttl_group = parser.add_mutually_exclusive_group()
  default_ttl_group.add_argument(
      '--default-ttl',
      type=arg_parsers.Duration(upper_bound=31622400),
      default=None,
      help=default_ttl_help,
  )
  if update_command:
    default_ttl_group.add_argument(
        '--no-default-ttl',
        action='store_true',
        help='Clears default TTL value.')
  max_ttl_help = """\
  Specifies the maximum allowed TTL for cached content served by this origin.

  The default value is 86400 for cache modes that support a max TTL.

  Cache directives that attempt to set a max-age or s-maxage higher than this,
  or an Expires header more than maxTtl seconds in the future, are capped at
  the value of maxTtl, as if it were the value of an s-maxage Cache-Control
  directive.

  A TTL of "0" means Always revalidate.

  The maximum allowed value is 31,622,400s (1 year). Infrequently
  accessed objects may be evicted from the cache before the defined TTL.

  When creating a new backend with CACHE_ALL_STATIC and the field is unset, or
  when updating an existing backend to use these modes and the field is unset,
  a default value of 86400 is used. When the cache mode is set to
  "USE_ORIGIN_HEADERS" or "FORCE_CACHE_ALL", you must omit this field.
  """
  max_ttl_group = parser.add_mutually_exclusive_group()
  max_ttl_group.add_argument(
      '--max-ttl',
      type=arg_parsers.Duration(upper_bound=31622400),
      default=None,
      help=max_ttl_help,
  )
  if update_command:
    max_ttl_group.add_argument(
        '--no-max-ttl', action='store_true', help='Clears max TTL value.')
  negative_caching_help = """\
  Negative caching allows per-status code cache TTLs to be set, in order to
  apply fine-grained caching for common errors or redirects. This can reduce
  the load on your origin and improve the end-user experience by reducing response
  latency.

  Negative caching applies to a set of 3xx, 4xx, and 5xx status codes that are
  typically useful to cache.

  Status codes not listed here cannot have their TTL explicitly set and aren't
  cached, in order to avoid cache poisoning attacks.

  HTTP success codes (HTTP 2xx) are handled by the values of defaultTtl and
  maxTtl.

  When the cache mode is set to CACHE_ALL_STATIC or USE_ORIGIN_HEADERS, these
  values apply to responses with the specified response code that lack any
  `cache-control` or `expires` headers.

  When the cache mode is set to FORCE_CACHE_ALL, these values apply to all
  responses with the specified response code, and override any caching headers.

  Cloud CDN applies the following default TTLs to these status codes:
  - HTTP 300 (Multiple Choice), 301, 308 (Permanent Redirects): 10m
  - HTTP 404 (Not Found), 410 (Gone), 451 (Unavailable For Legal Reasons): 120s
  - HTTP 405 (Method Not Found), 421 (Misdirected Request),
    501 (Not Implemented): 60s

  These defaults can be overridden in cdnPolicy.negativeCachingPolicy
  """
  negative_caching_group = parser.add_mutually_exclusive_group()
  if update_command:
    negative_caching_group.add_argument(
        '--negative-caching',
        action=arg_parsers.StoreTrueFalseAction,
        help=negative_caching_help)
  else:
    parser.add_argument(
        '--negative-caching',
        action=arg_parsers.StoreTrueFalseAction,
        help=negative_caching_help)
  negative_caching_policy_help = """\
  Sets a cache TTL for the specified HTTP status code.

  NegativeCaching must be enabled to config the negativeCachingPolicy.

  If you omit the policy and leave negativeCaching enabled, Cloud CDN's default
  cache TTLs are used.

  Note that when specifying an explicit negative caching policy, make sure that
  you specify a cache TTL for all response codes that you want to cache. Cloud
  CDN doesn't apply any default negative caching when a policy exists.

  *CODE* is the HTTP status code to define a TTL against. Only HTTP status codes
  300, 301, 308, 404, 405, 410, 421, 451, and 501 can be specified as values,
  and you cannot specify a status code more than once.

  TTL is the time to live (in seconds) for which to cache responses for the
  specified *CODE*. The maximum allowed value is 1800s (30 minutes), noting that
  infrequently accessed objects may be evicted from the cache before the defined TTL.
  """
  negative_caching_group.add_argument(
      '--negative-caching-policy',
      type=arg_parsers.ArgDict(key_type=int, value_type=int),
      metavar='[CODE=TTL]',
      help=negative_caching_policy_help)
  if update_command:
    negative_caching_group.add_argument(
        '--no-negative-caching-policies',
        action='store_true',
        help='Remove all negative caching policies for the %s.' % resource_name)
  custom_response_header_help = """\
  Custom headers that the external HTTP(S) load balancer adds to proxied responses.
  For the list of headers, see [Creating custom headers](https://cloud.google.com/load-balancing/docs/custom-headers).

  Variables are not case-sensitive.
  """
  custom_response_header_group = parser.add_mutually_exclusive_group()
  custom_response_header_group.add_argument(
      '--custom-response-header',
      action='append',
      help=custom_response_header_help)
  if update_command:
    custom_response_header_group.add_argument(
        '--no-custom-response-headers',
        action='store_true',
        help='Remove all custom response headers for the %s.' % resource_name)
