#!/usr/bin/env python
__author__ = 'Vladimir Budilov'

import gzip
import json
import logging
import sys
import time
from argparse import ArgumentParser

import boto3
from botocore.client import Config

import elastic

# filename='output.log',


kibanaStatus = "http://52.91.87.172:5601/status"
esStatus = "http://52.91.87.172:9200/"

es = elastic.ElasticSearch(connections="http://52.91.87.172:9200")

downloadedSnapshotFileName = "/tmp/configsnapshot" + str(time.time()) + ".json.gz"

regions = ['us-west-1', 'us-west-2', 'eu-west-1', 'us-east-1', 'eu-central-1', 'ap-southeast-1', 'ap-northeast-1',
           'ap-southeast-2', 'ap-northeast-2', 'sa-east-1']


def getBucketName(configConn):
    deliveryChannels = configConn.describe_delivery_channels()
    bucketName = None
    try:

        if deliveryChannels is not None and deliveryChannels.get("DeliveryChannels") is not None and len(
                deliveryChannels.get("DeliveryChannels")) > 0 and deliveryChannels.get("DeliveryChannels")[0].get(
            "s3BucketName") is not None:
            bucketName = deliveryChannels.get("DeliveryChannels")[0].get("s3BucketName")
    except:
        appLog.error("Couldn't retrieve the bucket name: " + str(sys.exc_info()))

    return bucketName


def deliverSnapshot(configConn):
    snapshotId = None

    # Check if the delivery channel is setup
    try:
        deliveryChannelsStatus = configConn.describe_delivery_channel_status()
        verboseLog.info("describe_delivery_channel_status result: " + str(deliveryChannelsStatus))
    except:
        appLog.error("This region is not setup properly for the configservice: " + str(sys.exc_info()))
        pass

    #
    if deliveryChannelsStatus is not None and deliveryChannelsStatus.get("DeliveryChannelsStatus") is not None and len(
            deliveryChannelsStatus.get("DeliveryChannelsStatus")) > 0:
        try:
            verboseLog.debug("getting the snapshot")
            snapshotResult = configConn.deliver_config_snapshot(deliveryChannelName='default')
            verboseLog.debug("snapshotResult: " + str(snapshotResult))

            snapshotId = str(snapshotResult.get("configSnapshotId"))
            verboseLog.debug("snapshotId: " + str(snapshotId))
        except:
            appLog.error("Couldn't deliver new snapshot: " + str(sys.exc_info()))
            return None

    return snapshotId


def getConfigurationSnapshotFile(s3Conn, bucketName, filePartialName):
    '''
    Loop through the files and find the current snapshot. Since s3 doesn't support regex lookups, I need to
    iterate over all of the items (and the config snapshot's name is not fully predictable because of the date value
    '''
    resultFile = None
    bucket = s3Conn.Bucket(str(bucketName))

    for key in bucket.objects.all():
        # logging.info("found: " + key.key)
        if filePartialName in key.key:
            resultFile = key.key
            verboseLog.info("Found the file in S3: " + resultFile)

    return resultFile


def loadDataIntoES(filename):
    with gzip.open(filename, 'rb') as dataFile:
        data = json.load(dataFile)

    if data is not None and data.get("configurationItems") is not None and len(data.get("configurationItems")) > 0:
        for item in data.get("configurationItems"):
            try:
                indexName = item.get("resourceType").lower()
                typeName = item.get("awsRegion")

                verboseLog.info("storing in ES: " + str(item))
                if addedIndexAndTypeDict.get(indexName) is not None and typeName not in addedIndexAndTypeDict.get(indexName):
                    pass
                else:
                    verboseLog.info("Setting the " + indexName + " index and " + typeName + " type to not_analyze")
                    es.setNotAnalyzed(indexName, typeName)

                    if addedIndexAndTypeDict.get(indexName) is None:
                        addedIndexAndTypeDict[indexName] = []
                    else:
                        addedIndexAndTypeDict[indexName].append(typeName)

                es.add(indexName, typeName, item.get("resourceId"), item)
            except:
                appLog.error("Couldn't add item: " + str(item) + " because " + str(sys.exc_info()))
                pass


def setNotAnalyzedOnESIndex(indexName):
    '''
    PUT /my_index
    {
      "mappings": {
        "my_type": {
            "dynamic_templates": [
                { "notanalyzed": {
                      "match":              "*",
                      "match_mapping_type": "string",
                      "mapping": {
                          "type":        "string",
                          "index":       "not_analyzed"
                      }
                   }
                }
              ]
           }
       }
    }
        '''


def main(region):
    myRegions = []
    if (region is not None):
        myRegions.append(region)
    else:
        myRegions = regions

    verboseLog.info("Looping through the regions")
    for curRegion in myRegions:
        appLog.info("Current region: " + curRegion)

        s3Conn = boto3.resource('s3', region_name=curRegion, config=Config(signature_version='s3v4'))
        s3Client = boto3.client('s3', region_name=curRegion, config=Config(signature_version='s3v4'))
        configConn = boto3.client('config', region_name=curRegion)

        verboseLog.info("Creating snapshot")
        snapshotId = deliverSnapshot(configConn)
        verboseLog.info("Got a new snapshot with an id of " + str(snapshotId))
        if snapshotId is None:
            continue

        bucketName = getBucketName(configConn)
        verboseLog.info("Using the following bucket name to search for the snapshot: " + str(bucketName))
        if bucketName is None:
            continue

        snapshotFilePath = getConfigurationSnapshotFile(s3Conn, bucketName, snapshotId)
        counter = 0
        if snapshotFilePath is None:
            while counter < 4:
                appLog.info("Waiting for the snapshot to appear")
                time.sleep(5)
                counter = counter + 1
                snapshotFilePath = getConfigurationSnapshotFile(s3Conn, bucketName, snapshotId)
                if snapshotFilePath is not None:
                    break

        verboseLog.info("snapshotFile: " + str(snapshotFilePath))
        if snapshotFilePath is None:
            continue

        verboseLog.info("Downloading the file to " + str(downloadedSnapshotFileName))
        s3Client.download_file(bucketName, snapshotFilePath, downloadedSnapshotFileName)

        verboseLog.info("Loading the file into elasticsearch")
        loadDataIntoES(downloadedSnapshotFileName)

        appLog.info("Successfully loaded data from " + curRegion)


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument('--region', '-r',
                        help='The region that needs to be analyzed. If left blank all regions will be analyzed.')

    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='If selected, the app spits out a lot of additional debug logs')
    args = parser.parse_args()

    addedIndexAndTypeDict = {}

    # Mute all other loggers
    logging.getLogger("root").setLevel(level=logging.FATAL)
    logging.getLogger("botocore.credentials").setLevel(level=logging.FATAL)
    logging.getLogger("botocore.vendored.requests.packages.urllib3.connectionpool").setLevel(level=logging.FATAL)
    logging.getLogger("boto3").setLevel(level=logging.FATAL)

    # Setup the main app logger
    appLog = logging.getLogger("app")
    appLog.setLevel(level=logging.INFO)

    # Setup the verbose logger
    verboseLog = logging.getLogger("verbose")
    if args.verbose:
        verboseLog.setLevel(level=logging.INFO)
    else:
        verboseLog.setLevel(level=logging.FATAL)

    main(args.region)
