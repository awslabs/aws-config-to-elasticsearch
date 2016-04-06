__author__ = 'Vladimir Budilov'

import gzip
import json
import logging
import sys
from argparse import ArgumentParser

import boto3

import elastic

kibanaStatus = "http://52.91.87.172:5601/status"
esStatus = "http://52.91.87.172:9200/"

es = elastic.ElasticSearch(connections="52.91.87.172:9200")

s3Conn = None
s3Client = None
configConn = None

downloadedSnapshotFileName = "/tmp/mysnapshot.json.gz"

regions = ['us-east-1', 'us-west-1', 'us-west-2', 'eu-west-1', 'eu-central-1', 'ap-southeast-1', 'ap-northeast-1',
           'ap-southeast-2', 'ap-northeast-2', 'sa-east-1']


def getBucketName():
    deliveryChannels = configConn.describe_delivery_channels()
    bucketName = None
    try:

        if deliveryChannels is not None and deliveryChannels.get("DeliveryChannels") is not None and len(
                deliveryChannels.get("DeliveryChannels")) > 0 and deliveryChannels.get("DeliveryChannels")[0].get(
            "s3BucketName") is not None:
            bucketName = deliveryChannels.get("DeliveryChannels")[0].get("s3BucketName")
    except:
        logging.error("Couldn't retrieve the bucket name: " + str(sys.exc_info()))

    return bucketName


def deliverSnapshot():
    snapshotId = None

    # Check if the delivery channel is setup
    try:
        deliveryChannelsStatus = configConn.describe_delivery_channel_status()
        logging.debug("describe_delivery_channel_status result: " + str(deliveryChannelsStatus))
    except:
        logging.error("This region is probably not setup properly for the configservice")
        return None

    #
    if deliveryChannelsStatus is not None and deliveryChannelsStatus.get("DeliveryChannelsStatus") is not None and len(
            deliveryChannelsStatus.get("DeliveryChannelsStatus")) > 0:
        try:
            logging.debug("getting the snapshot")
            snapshotResult = configConn.deliver_config_snapshot(deliveryChannelName='default')
            logging.debug("snapshotResult: " + str(snapshotResult))

            snapshotId = str(snapshotResult.get("configSnapshotId"))
            logging.debug("snapshotId: " + str(snapshotId))
        except:
            logging.error("Couldn't get the new snapshot: " + str(sys.exc_info()))
            return None

    return snapshotId


def getConfigurationSnapshotFile(bucket, filePartialName):
    '''
    Loop through the files and find the current snapshot. Since s3 doesn't support regex lookups, I need to
    iterate over all of the items (and the config snapshot's name is not fully predictable because of the date value
    '''
    resultFile = None
    bucket = s3Conn.Bucket(str(bucket))

    for key in bucket.objects.all():
        if filePartialName in key.key:
            resultFile = key.key
            print(key.key)

    return resultFile


def loadDataIntoES(filename):
    with gzip.open(filename, 'rb') as dataFile:
        data = json.load(dataFile)

    if data is not None and data.get("configurationItems") is not None and len(data.get("configurationItems")) > 0:
        for item in data.get("configurationItems"):
            try:
                es.add(item.get("resourceType").lower(), item.get("awsRegion"), item.get("resourceId"), item)
            except:
                logging.error("Couldn't add item: " + str(item) + " because " + str(sys.exc_info()))
                pass


def main():
    s3Conn = boto3.resource('s3')
    s3Client = boto3.client('s3')
    configConn = boto3.client('config')

    # snapshotId = deliverSnapshot()
    bucketName = getBucketName()
    snapshotId = "6d6807b5-e45e-43d1-a54e-46db834f8bac"
    snapshotFilePath = getConfigurationSnapshotFile(bucketName, snapshotId)
    # snapshotFile = s3Client.get_object(bucketName, snapshotFilePath)
    s3Client.download_file(bucketName, snapshotFilePath, downloadedSnapshotFileName)
    loadDataIntoES(downloadedSnapshotFileName)


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument('--file', '-f',
                        help='The configuration snapshot file that needs to be ingested')

    parser.add_argument('--enable', '-e',
                        help='The configuration snapshot file that needs to be ingested')

    args = parser.parse_args()

    # filename='output.log',
    logging.basicConfig(filename='output.log', level=logging.DEBUG,
                        format='%(asctime)s - %(name)s -  %(levelname)s - %(message)s')

    main()
