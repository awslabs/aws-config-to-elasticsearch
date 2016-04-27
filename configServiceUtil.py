'''
Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

__author__ = 'Vladimir Budilov'

import logging
import sys

import boto3


class ConfigServiceUtil():
    def __init__(self, region, verboseLog=None):
        self.region = region

        self.configConn = boto3.client('config', region_name=region)

        if verboseLog is None:
            self.verboseLog = logging.getLogger("configService")
            self.verboseLog.setLevel(level=logging.FATAL)
        else:
            self.verboseLog = verboseLog

    def getBucketNameFromConfigDeliveryChannel(self):
        deliveryChannels = self.configConn.describe_delivery_channels()
        bucketName = None
        try:
            if deliveryChannels is not None and deliveryChannels.get("DeliveryChannels") is not None and len(
                    deliveryChannels.get("DeliveryChannels")) > 0 and deliveryChannels.get("DeliveryChannels")[0].get(
                "s3BucketName") is not None:
                bucketName = deliveryChannels.get("DeliveryChannels")[0].get("s3BucketName")
        except:
            self.verboseLog.error("Couldn't retrieve the bucket name: " + str(sys.exc_info()))

        return bucketName

    def deliverSnapshot(self):
        snapshotId = None

        # Check if the delivery channel is setup
        try:
            deliveryChannelsStatus = self.configConn.describe_delivery_channel_status()
            self.verboseLog.info("describe_delivery_channel_status result: " + str(deliveryChannelsStatus))
        except:
            self.verboseLog.error("This region is not setup properly for the configservice: " + str(sys.exc_info()))
            pass

        #
        if deliveryChannelsStatus is not None and deliveryChannelsStatus.get(
                "DeliveryChannelsStatus") is not None and len(
            deliveryChannelsStatus.get("DeliveryChannelsStatus")) > 0:
            try:
                self.verboseLog.debug("getting the snapshot")
                snapshotResult = self.configConn.deliver_config_snapshot(deliveryChannelName='default')
                self.verboseLog.debug("snapshotResult: " + str(snapshotResult))

                snapshotId = str(snapshotResult.get("configSnapshotId"))
                self.verboseLog.debug("snapshotId: " + str(snapshotId))
            except:
                self.verboseLog.error("Couldn't deliver new snapshot. Maybe you're being throttled. " + str(sys.exc_info()))

        return snapshotId
