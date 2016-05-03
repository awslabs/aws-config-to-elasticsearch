import logging
import sys

import boto3

class ConfigServiceUtil(object):
    def __init__(self, region, verbose_log=None):
        self.region = region

        self.config_conn = boto3.client('config', region_name=region)

        if verbose_log is None:
            self.verbose_log = logging.getLogger("configService")
            self.verbose_log.setLevel(level=logging.FATAL)
        else:
            self.verbose_log = verbose_log

    def get_bucket_name_from_config_delivery_channel(self):
        delivery_channels = self.config_conn.describe_delivery_channels()
        bucket_name = None
        try:
            if delivery_channels.get("DeliveryChannels") is not None \
                    and len(delivery_channels.get("DeliveryChannels")) > 0 \
                    and delivery_channels.get("DeliveryChannels")[0].get("s3BucketName") is not None:
                bucket_name = delivery_channels.get("DeliveryChannels")[0].get("s3BucketName")
        except:
            self.verbose_log.error(
                "Couldn't retrieve the bucket name: " + str(sys.exc_info()))

        return bucket_name

    def deliver_snapshot(self):
        snapshotid = None

        # Check if the delivery channel is setup
        deliverychannelsstatus = None
        try:
            deliverychannelsstatus = self.config_conn.describe_delivery_channel_status()
            self.verbose_log.info(
                "describe_delivery_channel_status result: " + str(
                    deliverychannelsstatus))
        except:
            self.verbose_log.error(
                "This region is not setup properly for the configservice: " + str(sys.exc_info()))
            return None

        #
        if deliverychannelsstatus is not None and deliverychannelsstatus.get(
                "DeliveryChannelsStatus") is not None and len(deliverychannelsstatus.get("DeliveryChannelsStatus")) > 0:
            try:
                self.verbose_log.debug("getting the snapshot")
                snapshotresult = self.config_conn.deliver_config_snapshot(
                    deliveryChannelName='default')
                self.verbose_log.debug("snapshotResult: " + str(snapshotresult))

                snapshotid = str(snapshotresult.get("configSnapshotId"))
                self.verbose_log.debug("snapshotId: " + str(snapshotid))
            except:
                self.verbose_log.error(
                    "Couldn't deliver new snapshot. Maybe you're being throttled. " + str(sys.exc_info()))

        return snapshotid
