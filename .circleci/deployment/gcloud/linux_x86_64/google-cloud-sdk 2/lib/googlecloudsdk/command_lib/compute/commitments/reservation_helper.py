# -*- coding: utf-8 -*- #
# Copyright 2018 Google LLC. All Rights Reserved.
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
"""Helpers for creating reservation within commitment creation."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute.reservations import resource_args
from googlecloudsdk.command_lib.compute.reservations import util
from googlecloudsdk.core import yaml


def MakeReservations(args, messages, holder):
  if args.IsSpecified('reservations_from_file'):
    return _MakeReservationsFromFile(messages, args)
  elif args.IsSpecified('reservation'):
    return [_MakeSingleReservation(args, messages, holder)]
  else:
    return []


def MakeUpdateReservations(args, messages):
  if args.IsSpecified('reservations_from_file'):
    return _MakeReservationsFromFile(messages, args)
  elif args.IsSpecified('source_reservation'):
    return MakeSourceDestReservations(args, messages)
  else:
    return []


def MakeSourceDestReservations(args, messages):
  """Return messages required for update-reservations command."""
  source_msg = ReservationArgToMessage('source_reservation',
                                       'source_accelerator',
                                       'source_local_ssd',
                                       args,
                                       messages)
  destination_msg = ReservationArgToMessage('dest_reservation',
                                            'dest_accelerator',
                                            'dest_local_ssd',
                                            args,
                                            messages)
  return [source_msg, destination_msg]


def ReservationArgToMessage(reservation, accelerator, local_ssd,
                            args, messages):
  """Convert single reservation argument into a message."""
  accelerators = util.MakeGuestAccelerators(messages,
                                            getattr(args, accelerator,
                                                    None))
  local_ssds = util.MakeLocalSsds(messages, getattr(args, local_ssd,
                                                    None))
  reservation = getattr(args, reservation, None)
  specific_allocation = util.MakeSpecificSKUReservationMessage(
      messages, reservation.get('vm-count', None),
      accelerators, local_ssds,
      reservation.get('machine-type', None),
      reservation.get('min-cpu-platform', None))
  a_msg = util.MakeReservationMessage(
      messages, reservation.get('reservation', None),
      specific_allocation,
      reservation.get('require-specific-reservation', None),
      reservation.get('reservation-zone', None))

  return a_msg


def _MakeReservationsFromFile(messages, args):
  reservations_yaml = yaml.load(args.reservations_from_file)
  return _ConvertYAMLToMessage(messages, reservations_yaml)


def _ConvertYAMLToMessage(messages, reservations_yaml):
  """Converts the fields in yaml to allocation message object."""
  if not reservations_yaml:
    return []
  allocations_msg = []
  for a in reservations_yaml:
    accelerators = util.MakeGuestAccelerators(messages,
                                              a.get('accelerator', None))
    local_ssds = util.MakeLocalSsds(messages, a.get('local_ssd', None))
    specific_allocation = util.MakeSpecificSKUReservationMessage(
        messages, a.get('vm_count', None), accelerators, local_ssds,
        a.get('machine_type', None), a.get('min_cpu_platform', None))
    a_msg = util.MakeReservationMessage(
        messages, a.get('reservation', None), specific_allocation,
        a.get('require_specific_reservation', None),
        a.get('reservation_zone', None))
    allocations_msg.append(a_msg)
  return allocations_msg


def _MakeSingleReservation(args, messages, holder):
  """Makes one Allocation message object."""
  reservation_ref = resource_args.GetReservationResourceArg(
      positional=False).ResolveAsResource(
          args,
          holder.resources,
          scope_lister=compute_flags.GetDefaultScopeLister(holder.client))
  return util.MakeReservationMessageFromArgs(messages, args, reservation_ref)
