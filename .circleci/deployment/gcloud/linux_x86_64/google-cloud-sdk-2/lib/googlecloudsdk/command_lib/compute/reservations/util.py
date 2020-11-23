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
"""Common utility functions to consturct compute reservations message."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


def MakeReservationMessageFromArgs(messages, args, allocation_ref):
  accelerators = MakeGuestAccelerators(messages,
                                       getattr(args, 'accelerator', None))
  local_ssds = MakeLocalSsds(messages, getattr(args, 'local_ssd', None))
  specific_reservation = MakeSpecificSKUReservationMessage(
      messages, args.vm_count, accelerators, local_ssds, args.machine_type,
      args.min_cpu_platform, getattr(args, 'location_hint', None),
      getattr(args, 'maintenance_freeze_duration', None),
      getattr(args, 'maintenance_interval', None))
  return MakeReservationMessage(
      messages, allocation_ref.Name(), specific_reservation,
      args.require_specific_reservation, allocation_ref.zone)


def MakeGuestAccelerators(messages, accelerator_configs):
  """Constructs the repeated accelerator message objects."""
  if accelerator_configs is None:
    return []

  accelerators = []

  for a in accelerator_configs:
    m = messages.AcceleratorConfig(
        acceleratorCount=a['count'], acceleratorType=a['type'])
    accelerators.append(m)

  return accelerators


def MakeLocalSsds(messages, ssd_configs):
  """Constructs the repeated local_ssd message objects."""
  if ssd_configs is None:
    return []

  local_ssds = []
  disk_msg = (
      messages
      .AllocationSpecificSKUAllocationAllocatedInstancePropertiesReservedDisk)
  interface_msg = disk_msg.InterfaceValueValuesEnum

  for s in ssd_configs:
    if s['interface'].upper() == 'NVME':
      interface = interface_msg.NVME
    else:
      interface = interface_msg.SCSI
    m = disk_msg(diskSizeGb=s['size'], interface=interface)
    local_ssds.append(m)

  return local_ssds


def MakeSpecificSKUReservationMessage(
    messages, vm_count, accelerators, local_ssds, machine_type,
    min_cpu_platform, location_hint=None, freeze_duration=None,
    freeze_interval=None):
  """Constructs a single specific sku reservation message object."""
  prop_msgs = (
      messages.AllocationSpecificSKUAllocationReservedInstanceProperties)
  instance_properties = prop_msgs(
      guestAccelerators=accelerators,
      localSsds=local_ssds,
      machineType=machine_type,
      minCpuPlatform=min_cpu_platform)
  if freeze_duration:
    instance_properties.maintenanceFreezeDurationHours = freeze_duration // 3600
  if freeze_interval:
    instance_properties.maintenanceInterval = (
        messages.AllocationSpecificSKUAllocationReservedInstanceProperties.
        MaintenanceIntervalValueValuesEnum(freeze_interval))
  if location_hint:
    instance_properties.locationHint = location_hint

  return messages.AllocationSpecificSKUReservation(
      count=vm_count, instanceProperties=instance_properties)


def MakeReservationMessage(messages, reservation_name, specific_reservation,
                           require_specific_reservation, reservation_zone):
  """Constructs a single allocation message object."""
  return messages.Reservation(
      name=reservation_name,
      specificReservation=specific_reservation,
      specificReservationRequired=require_specific_reservation,
      zone=reservation_zone)
