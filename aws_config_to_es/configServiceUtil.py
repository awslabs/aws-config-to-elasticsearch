"""
Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
and limitations under the License.
"""

import logging
import sys

import boto3


class ConfigServiceUtil(object):
    def __init__(self, region, verbose_log=None):
        self.region = region

        self.configConn = boto3.client('config', region_name=region)

        if verbose_log is None:
            self.verboseLog = logging.getLogger("configService")
            self.verboseLog.setLevel(level=logging.FATAL)
        else:
            self.verboseLog = verbose_log

    def get_bucket_name_from_config_delivery_channel(self):
        delivery_channels = self.configConn.describe_delivery_channels()
        bucket_name = None
        try:
            if delivery_channels is not None \
                    and delivery_channels.get("DeliveryChannels") is not None \
                    and len(delivery_channels.get("DeliveryChannels")) > 0 \
                    and delivery_channels.get("DeliveryChannels")[0].get("s3BucketName") is not None:
                bucket_name = delivery_channels.get("DeliveryChannels")[0].get("s3BucketName")
        except:
            self.verboseLog.error(
                "Couldn't retrieve the bucket name: " + str(sys.exc_info()))

        return bucket_name

    def deliver_snapshot(self):
        snapshotid = None

        # Check if the delivery channel is setup
        deliverychannelsstatus = None
        try:
            deliverychannelsstatus = self.configConn.describe_delivery_channel_status()
            self.verboseLog.info(
                "describe_delivery_channel_status result: " + str(
                    deliverychannelsstatus))
        except:
            self.verboseLog.error(
                "This region is not setup properly for the configservice: " + str(sys.exc_info()))
            pass

        #
        if deliverychannelsstatus is not None and deliverychannelsstatus.get(
                "DeliveryChannelsStatus") is not None and len(deliverychannelsstatus.get("DeliveryChannelsStatus")) > 0:
            try:
                self.verboseLog.debug("getting the snapshot")
                snapshotresult = self.configConn.deliver_config_snapshot(
                    deliveryChannelName='default')
                self.verboseLog.debug("snapshotResult: " + str(snapshotresult))

                snapshotid = str(snapshotresult.get("configSnapshotId"))
                self.verboseLog.debug("snapshotId: " + str(snapshotid))
            except:
                self.verboseLog.error(
                    "Couldn't deliver new snapshot. Maybe you're being throttled. " + str(sys.exc_info()))

        return snapshotid
