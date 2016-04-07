__author__ = 'Vladimir Budilov'

import datetime
import json
import logging
import sys

import elasticsearch
import requests


class ElasticSearch():
    def __init__(self, connections=None, sniffOnStart=False):
        logging.basicConfig(level=logging.INFO)

        if not connections:
            self.connections = ("localhost:9200")
        else:
            self.connections = connections

        logging.info("Setting up the initial connection")

        self.es = elasticsearch.Elasticsearch(self.connections, sniff_on_start=sniffOnStart,
                                              sniff_on_connection_fail=False,
                                              sniffer_timeout=60)

    def add(self, indexName, docType, indexId, jsonMessage):
        """ Returns the id of the newly inserted value or None """

        """ if the added date is not there, then I'm adding it in """
        if not isinstance(jsonMessage, dict):
            jsonMessageDict = json.loads(jsonMessage)
        else:
            jsonMessageDict = jsonMessage

        addedIso = None
        if jsonMessageDict.get("addedIso"):
            logging.debug("Nothing to do...addedIso is already here")
        elif jsonMessageDict.get("added"):
            try:
                logging.debug("checking added field")
                added = int(jsonMessageDict.get("added")) / 1000
                logging.debug("added: " + str(added))
                if added:
                    jsonMessageDict["addedIso"] = datetime.datetime.fromtimestamp(added).isoformat()
            except:
                logging.error("Exception in search: " + sys.exc_info())
        else:
            logging.debug("addedIso isn't there..adding from scratch")
            jsonMessageDict["addedIso"] = datetime.datetime.now().isoformat()

        updatedIso = None
        if "updated" in jsonMessageDict.keys():
            try:
                updated = int(jsonMessageDict["updated"]) / 1000
                if updated:
                    updatedIso = datetime.datetime.fromtimestamp(updated).isoformat()

            except:
                logging.error("Exception in search: " + sys.exc_info())

        if not updatedIso:
            updatedIso = datetime.datetime.now().isoformat()

        jsonMessageDict["updatedIso"] = updatedIso

        jsonMessage = json.dumps(jsonMessageDict)

        if indexId:
            response = self.es.index(
                index=indexName,
                doc_type=docType,
                id=indexId,
                body=jsonMessage
            )
        else:
            response = self.es.index(
                index=indexName,
                doc_type=docType,
                body=jsonMessage
            )
        logging.info("response: " + str(response))

        responseId = None
        try:
            responseId = response["_id"]
        except:
            pass

        return responseId

    def setNotAnalyzed(self, indexName, typeName):
        '''
        Sets the indexName and typeName as not_analyzed, which means that the data
        won't be tokenized, and therefore can be searched by the value itself
        '''


        payload = {
            "mappings": {
                typeName: {
                    "dynamic_templates": [
                        {
                            "strings": {
                                "match_mapping_type": "string",
                                "mapping": {
                                    "type": "string",
                                    "fields": {
                                        "raw": {
                                            "type":  "string",
                                            "index": "not_analyzed",
                                            "ignore_above": 256
                                        }
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
        print("Sending request")
        result = requests.put(self.connections + "/" + indexName, payload)
        print result.text
if __name__ == "__main__":
    es = ElasticSearch(connections="http://52.91.87.172:9200")
    es.setNotAnalyzed("aws::ec2::aainternetgateway", "us-west-2")
