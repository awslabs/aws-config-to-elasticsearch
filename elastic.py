__author__ = 'Vladimir Budilov'

import logging
import sys
import datetime
import json

import requests
import elasticsearch


'''
This module should simplify the interaction with elasticsearch.
'''
class ElasticSearchMeta():
    '''
    Simple class to retrieve the elasticsearch meta-data.
    '''

    def __init__(self, connections=None):
        logging.basicConfig(level=logging.INFO)

        if not connections:
            connections = ("localhost:9200")

        self.connections = connections
        logging.info("Setting up the initial connection for ElasticSearchMeta")

        self.es = elasticsearch.Elasticsearch(connections, sniff_on_start=True, sniff_on_connection_fail=True,
                                              sniffer_timeout=60)

    def getIndices(self):
        response = requests.get("http://" + self.connections[0] + "/_mapping")

        return json.loads(response.text).keys()

    def getTypesForIndex(self, index):
        data = dict()

        response = requests.get("http://" + self.connections[0] + "/_mapping")

        try:
            if index in json.loads(response.text).keys():
                return json.loads(response.text)[index]["mappings"].keys()

        except:
            logging.error("Couldn't find the index")
            return None

    def getFieldsForType(self, index, type):
        data = dict()

        response = requests.get("http://" + self.connections[0] + "/_mapping")

        try:
            if index in json.loads(response.text).keys():
                return json.loads(response.text)[index]["mappings"][type]["properties"].keys()

        except:
            logging.error("Couldn't find the index")
            return None

    # def getFieldsForTypes(self, indexAndType):
    # '''
    #
    # :param indexAndType: a comma-separated array of index
    # :return:
    # '''
    # data = dict()
    #
    #     response = requests.get("http://localhost:9200/_mapping")
    #
    #     try:
    #         if index in json.loads(response.text).keys():
    #             return json.loads(response.text)[index]["mappings"][type]["properties"].keys()
    #
    #     except:
    #         logging.error("Couldn't find the index")
    #         return None

    def getMappings(self):
        '''

        :return:
            {
                "test_index": {
                    "mappings": {
                        "test_type": {
                            "properties": {
                                "test_key": {
                                    "type": "string"
                                },
                                "test_key4": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            }
        '''
        response = requests.get("http://localhost:9200/_mapping")
        if response and response.text:
            return json.loads(response.text)
        else:
            logging.error("Couldn't retrieve the mappings info")
            return None


class ElasticSearch():
    def __init__(self, connections=None, sniffOnStart=False):
        logging.basicConfig(level=logging.INFO)

        if not connections:
            connections = ("localhost:9200")
        logging.info("Setting up the initial connection")
        self.es = elasticsearch.Elasticsearch(connections, sniff_on_start=sniffOnStart, sniff_on_connection_fail=True,
                                              sniffer_timeout=60)


    def add(self, indexName, docType, indexId, jsonMessage):
        """ Returns the id of the newly inserted value or None """
        # Index a document:
        # es.index(
        # index="my_app",
        # doc_type="blog_post",
        # id=1,
        # body={
        # "title": "Elasticsearch clients",
        # "content": "Interesting content...",
        # "date": date(2013, 9, 24),
        # }
        # )
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

    def getById(self, indexName, docType, indexId):
        if not indexId:
            raise "indexId is null"

        # Get the document:
        response = None
        try:
            response = self.es.get(index=indexName, doc_type=docType, id=indexId)
        except:
            logging.warn("Couldn't get the item")

        logging.info("getById response: " + str(response))
        if response:
            response["_source"]["id"] = indexId

            return response["_source"]
        else:
            return None

    def delete(self, indexName, docType, indexId):
        response = None
        try:
            response = self.es.delete(index=indexName, doc_type=docType, id=indexId)

            if response or not response["ok"]:
                response = True
        except:
            response = None

        return response

    def searchByDateRange(self, dateFieldname, dateFrom, dateTo, additionalQueryParams, index=None, docType=None):

        if not dateFrom or not dateTo or not dateFieldname:
            raise Exception("The from and to dates, and fieldName are required fields")

        '''
            Sample or query with range:
                            data = {"query": {"filtered": {"filter": {"or": [{
                                                     "term": {"deviceType": "qamipstb"}
                                                 },
                                                 {
                                                     "term": {"deviceType": "ipstb"}
                                                 }]}, "query": {"match_all": {}}}}}

        :param index: if left None then all of the indexes are searched
        :param type: if left None or index is None then all of the indexes and types are searched
        :param dateFrom: isoFormat - 2014-04-17T12:00:00
        :param dateTo: isoFormat - 2014-04-17T12:00:00
        :return: the first set of results

        '''

        query = {
            "query": {
                "range": {
                    dateFieldname: {
                        "gte": dateFrom,
                        "lte": dateTo
                    }
                }
            }
        }

        return self.search(index, query, docType=docType)


    def search(self, indexName, searchQuery, docType=None, rawQuery=False, justResults=False, justIds=False,
               returnTotalCount=False,
               fieldsToReturn=None, size=100, offset=0):
        '''

        :param indexName:
        :param searchQuery:
        :param docType:
        :param rawQuery:
        :param justResults: If True then returns only the json with the array of the results, without the additional meta-data
        :param returnTotalCount: If set to True then total count of results will be returned along with the data
        :return: Returns the "hits" response based on the search query.

        searchQuery should be json, NOT a dictionary

        curl -X GET "http://localhost:9200/fashion/_search?q=user:test&pretty=true"
        {
          "took" : 4,
          "timed_out" : false,
          "_shards" : {
            "total" : 5,
            "successful" : 5,
            "failed" : 0
          },
          "hits" : {
            "total" : 2,
            "max_score" : 1.6931472,
            "hits" : [ {
              "_index" : "fashion",
              "_type" : "user",
              "_id" : "jxzVEoKuR2i8ka5SkS2cGA",
              "_score" : 1.6931472, "_source" : {"user": "test"}
            }, {
              "_index" : "fashion",
              "_type" : "user",
              "_id" : "68irjGFbRGSA0xY5KfZhdA",
              "_score" : 1.6931472, "_source" : {"user": "test"}
            } ]
          }
        }


        Returns the following array:
        [ {
              "_index" : "fashion",
              "_type" : "user",
              "_id" : "jxzVEoKuR2i8ka5SkS2cGA",
              "_score" : 1.6931472,
              "_source" : {"user": "test"}
            }, {
              "_index" : "fashion",
              "_type" : "user",
              "_id" : "68irjGFbRGSA0xY5KfZhdA",
              "_score" : 1.6931472,
              "_source" : {"user": "test"}
            } ]

        if nothing was found, then return None
        '''

        totalCount = 0

        if rawQuery:
            searchString = searchQuery
        else:
            searchString = {"query": {"match": searchQuery}}

        logging.info("search string: " + str(searchString))

        try:

            data = self.es.search(index=indexName, doc_type=docType, body=searchString, size=size,
                                  from_=offset)

            logging.info("returned data in search: " + str(data))
            if not data or not int(data["hits"]["total"]) > 0:
                logging.info("nothing found: " + str(data["hits"]["total"]))
                response = None
            else:
                logging.info(" found: " + str(data["hits"]["total"]))
                if returnTotalCount:
                    totalCount = int(data["hits"]["total"])

                if justResults:
                    response = []
                    for item in data["hits"]["hits"]:
                        resWithId = item.get("_source")
                        resWithId["id"] = item.get("_id")
                        response.append(resWithId)
                elif justIds:
                    response = []
                    for item in data["hits"]["hits"]:
                        response.append(item.get("_id"))
                else:
                    response = data.get("hits")
        except:
            print("Exception in search: ", sys.exc_info())
            logging.error("Exception in search: " + str(sys.exc_info()))
            response = None

        if returnTotalCount:
            return totalCount, response
        else:
            return response