__author__ = 'Vladimir Budilov'

import datetime
import json
import logging
import requests


class ElasticSearch():
    def __init__(self, connections=None, log=None):

        if not connections:
            self.connections = ("localhost:9200")
        else:
            self.connections = connections

        if log is not None:
            self.log = log
        else:
            self.log = logging.getLogger("elastic")

        self.log.debug("Setting up the initial connection")

    def add(self, indexName=None, docType=None, indexId=None, jsonMessage=None):
        """ Returns the id of the newly inserted value or None """

        """ if the added date is not there, then I'm adding it in """
        if not isinstance(jsonMessage, dict):
            jsonMessageDict = json.loads(jsonMessage)
        else:
            jsonMessageDict = jsonMessage

        jsonMessageDict["addedIso"] = datetime.datetime.now().isoformat()
        jsonMessageDict["updatedIso"] = datetime.datetime.now().isoformat()

        jsonMessage = json.dumps(jsonMessageDict)

        if indexId:
            response = requests.put(self.connections + "/" + indexName + "/" + docType + "/" + indexId,
                                    data=jsonMessage)
        else:
            response = requests.post(self.connections + "/" + indexName + "/" + docType, data=jsonMessage)
        self.log.info("response: " + str(response) + "...message: " + str(response.content))

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
