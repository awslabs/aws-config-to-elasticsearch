__author__ = 'Vladimir Budilov'

import datetime
import json
import logging
import requests
import sys


class ElasticSearch():
    def __init__(self, connections=None, sniffOnStart=False):
        logging.basicConfig(level=logging.INFO)

        if not connections:
            self.connections = ("localhost:9200")
        else:
            self.connections = connections

        logging.info("Setting up the initial connection")

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
            response = requests.put(self.connections + "/" + indexName + "/" + docType + "/" + indexId,
                                    data=jsonMessage)
        else:
            response = requests.put(self.connections + "/" + indexName + "/" + docType, data=jsonMessage)
        # logging.info("response: " + str(response))

        responseId = None
        try:
            responseId = response["_id"]
        except:
            pass

        return responseId

    def setNotAnalyzedTemplate(self):
        '''
        Sets the indexName and typeName as not_analyzed, which means that the data
        won't be tokenized, and therefore can be searched by the value itself
        '''

        payload = {
            "template": "*",
            "settings": {
                "index.refresh_interval": "5s"
            },
            "mappings": {
                "_default_": {
                    "_all": {"enabled": True},
                    "dynamic_templates": [{
                        "string_fields": {
                            "match": "*",
                            "match_mapping_type": "string",
                            "mapping": {
                                "type": "string", "index": "analyzed", "omit_norms": True,
                                "fields": {
                                    "raw": {"type": "string", "index": "not_analyzed", "ignore_above": 256}
                                }
                            }
                        }
                    }]
                }
            }
        }
        requests.put(self.connections + "/_template/configservice", data=json.dumps(payload))
