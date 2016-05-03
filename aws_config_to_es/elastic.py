import datetime
import json
import logging
import requests


class ElasticSearch(object):
    def __init__(self, connections=None, log=None):

        if connections is None:
            self.connections = "localhost:9200"
        else:
            self.connections = connections

        if log is not None:
            self.log = log
        else:
            self.log = logging.getLogger("elastic")

        self.log.debug("Setting up the initial connection")

    def add(
            self,
            index_name=None,
            doc_type=None,
            index_id=None,
            json_message=None):
        # Returns the id of the newly inserted value or None

        # if the added date is not there, then I'm adding it in
        if not isinstance(json_message, dict):
            json_message_dict = json.loads(json_message)
        else:
            json_message_dict = json_message

        json_message_dict["addedIso"] = datetime.datetime.now().isoformat()
        json_message_dict["updatedIso"] = json_message_dict["addedIso"]

        json_message = json.dumps(json_message_dict)

        self.log.info("adding item into ES: " + str(json_message_dict))
        if index_id:
            response = requests.put(
                self.connections + "/" + index_name +
                "/" + doc_type + "/" + index_id,
                data=json_message)
        else:
            response = requests.post(
                self.connections +
                "/" +
                index_name +
                "/" +
                doc_type,
                data=json_message)

        self.log.info(
            "response: " + str(
                response.content) + "...message: " + str(
                response.content))

        responseid = None

        try:
            responseid = json.loads(response.content).get("_id")
        except:
            pass

        return responseid

    def set_not_analyzed_template(self):
        """
        Sets the indexName and typeName as not_analyzed, which means that the data
        won't be tokenized, and therefore can be searched by the value itself
        """

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
                                "type": "string",
                                "index": "analyzed",
                                "omit_norms": True,
                                "fields": {
                                    "raw": {"type": "string", "index": "not_analyzed", "ignore_above": 256}
                                }
                            }
                        }
                    }]
                }
            }
        }
        requests.put(
            self.connections + "/_template/configservice",
            data=json.dumps(payload))
