__author__ = 'Vladimir Budilov'

import logging
import sys

import boto3


class ConfigServiceUtil():
    def __init__(self, region, log=None, verbose=False):
        self.region = region

        self.configConn = boto3.client('config', region_name=region)
        self.verboseLog = logging.getLogger("verbose")
        if verbose:
            self.verboseLog.setLevel(level=logging.INFO)
        else:
            self.verboseLog.setLevel(level=logging.FATAL)

        if log is None:
            self.appLog = self.verboseLog
        else:
            self.appLog = log

    def getBucketNameFromConfigDeliveryChannel(self):
        deliveryChannels = self.configConn.describe_delivery_channels()
        bucketName = None
        try:
            if deliveryChannels is not None and deliveryChannels.get("DeliveryChannels") is not None and len(
                    deliveryChannels.get("DeliveryChannels")) > 0 and deliveryChannels.get("DeliveryChannels")[0].get(
                "s3BucketName") is not None:
                bucketName = deliveryChannels.get("DeliveryChannels")[0].get("s3BucketName")
        except:
            self.appLog.error("Couldn't retrieve the bucket name: " + str(sys.exc_info()))

        return bucketName

    def deliverSnapshot(self):
        snapshotId = None

        # Check if the delivery channel is setup
        try:
            deliveryChannelsStatus = self.configConn.describe_delivery_channel_status()
            self.verboseLog.info("describe_delivery_channel_status result: " + str(deliveryChannelsStatus))
        except:
            self.appLog.error("This region is not setup properly for the configservice: " + str(sys.exc_info()))
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
                self.appLog.error("Couldn't deliver new snapshot. Maybe you're being throttled. " + str(sys.exc_info()))

        return snapshotId
