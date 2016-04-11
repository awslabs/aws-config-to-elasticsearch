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
from configServiceUtil import ConfigServiceUtil

kibanaStatus = "http://52.91.87.172:5601/status"
esStatus = "http://52.91.87.172:9200/"

downloadedSnapshotFileName = "/tmp/configsnapshot" + str(time.time()) + ".json.gz"

regions = ['us-west-1', 'us-west-2', 'eu-west-1', 'us-east-1', 'eu-central-1', 'ap-southeast-1', 'ap-northeast-1',
           'ap-southeast-2', 'ap-northeast-2', 'sa-east-1']


def getConfigurationSnapshotFile(s3Conn, bucketName, filePartialName):
    '''
    Returns the name of the configuration file from S3

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
                if addedIndexAndTypeDict.get(indexName) is not None and typeName not in addedIndexAndTypeDict.get(
                        indexName):
                    pass
                else:
                    verboseLog.info("Setting the " + indexName + " index and " + typeName + " type to not_analyze")

                    if addedIndexAndTypeDict.get(indexName) is None:
                        addedIndexAndTypeDict[indexName] = []
                    else:
                        addedIndexAndTypeDict[indexName].append(typeName)

                es.add(indexName, typeName, item.get("resourceId"), item)
            except:
                appLog.error("Couldn't add item: " + str(item) + " because " + str(sys.exc_info()))
                pass


def main(region):
    # This call is light, so it's ok not to check whether the template already exists
    es.setNotAnalyzedTemplate()

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
        configService = ConfigServiceUtil(region=curRegion, log=appLog)

        verboseLog.info("Creating snapshot")
        snapshotId = configService.deliverSnapshot()
        verboseLog.info("Got a new snapshot with an id of " + str(snapshotId))
        if snapshotId is None:
            appLog.info("AWS Config isn't setup in " + curRegion)
            continue

        bucketName = configService.getBucketNameFromConfigDeliveryChannel()
        verboseLog.info("Using the following bucket name to search for the snapshot: " + str(bucketName))
        if bucketName is None:
            appLog.error("Couldn't search an S3 bucket -- are you sure your permissions are setup correctly?")
            continue

        snapshotFilePath = getConfigurationSnapshotFile(s3Conn, bucketName, snapshotId)
        counter = 0
        if snapshotFilePath is None:
            while counter < 5:
                appLog.info(" " + str(counter) + " - Waiting for the snapshot to appear")
                time.sleep(10)
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
    parser.add_argument('--destination', '-d', required=True,
                        help='The ip:port of the elastic search instance')
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='If selected, the app runs in verbose mode -- a lot more logs!')
    args = parser.parse_args()

    addedIndexAndTypeDict = {}

    # Mute all other loggers
    logging.getLogger("root").setLevel(level=logging.FATAL)
    logging.getLogger("botocore.credentials").setLevel(level=logging.FATAL)
    logging.getLogger("botocore.vendored.requests.packages.urllib3.connectionpool").setLevel(level=logging.FATAL)
    logging.getLogger("boto3").setLevel(level=logging.FATAL)
    logging.getLogger("requests").setLevel(level=logging.FATAL)

    # Setup the main app logger
    appLog = logging.getLogger("app")
    appLog.setLevel(level=logging.INFO)

    if args.destination is None:
        appLog.error("You need to enter the IP of your ElasticSearch instance")
        exit()
    else:
        destination = "http://" + args.destination

    es = elastic.ElasticSearch(connections=destination)

    # Setup the verbose logger
    verboseLog = logging.getLogger("verbose")
    if args.verbose:
        verboseLog.setLevel(level=logging.INFO)
    else:
        verboseLog.setLevel(level=logging.FATAL)

    main(args.region)
