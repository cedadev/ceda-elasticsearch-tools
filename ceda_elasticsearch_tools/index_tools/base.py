# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '07 Jun 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


import json
import hashlib
from tqdm import tqdm
import sys
import os
from elasticsearch.helpers import scan
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient


class IndexUpdaterBase(object):
    """
    Base class for index updaters. Contains common methods.
    """

    def __init__(self, index, **kwargs):
        """
        Common variables.
        :param index:   Index to update
        """

        ca_root = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), '../root_certificate/root-ca.pem')
        )

        self.index = index
        self.es = CEDAElasticsearchClient(**kwargs)

    @staticmethod
    def _get_action_key(es_response_item):
        """
        Get the action key for processing the response
        :param es_response_item:
        :return: key
        """

        actions = ["update", "index", "delete"]
        response_keys = es_response_item.keys()

        return list(set(actions) & set(response_keys))[0]

    def _scroll_search(self, query, size=1000):
        """
        Perform a scroll search query

        :param query:   The query to perform
        :param size:    Size to return in each scroll. (default: 1000)
        :return:        Generator of results
        """

        return scan(self.es, query=query, scroll='1m', index=self.index, size=size)

    def _bulk_action(self, action_list, api="bulk", process_results=True):
        """
        Perform bulk action to elasticsearch. This is either bulk|msearch. Defualt: bulk

        :param action_list: List of bulk index operations.
        :return Consolidated report.
                    when api == bulk    returns {"success": int, "failed": int, "failed_items": list}
                    when api == msearch returns list with three levels as described below
                    [           # Container for the reponse
                        [       # Collection of all the responses in a block as submitted to elasticsearch
                            []  # Indiviual query responses
                        ]
                    ]

        """

        response_list = []
        for action in tqdm(action_list, desc="Processing queries", file=sys.stdout):

            if api == "bulk":
                response = self.es.bulk(index=self.index, body=action)
            elif api == "msearch":
                response = self.es.msearch(body=action)
            else:
                raise ValueError("Invalid api selected. Must be of either bulk|msearch")

            response_list.append(response)

        return self._process_bulk_action_response(response_list, api, process=process_results)

    def _generate_bulk_operation_body(self, content_list, action="index"):
        """
        Generate the query body for the bulk operation

        :param content_list:    List of dictionaries containing the content to be actioned upon
        :param action:          The elasticsearch action to perform. (index|update|delete) (default: index)
        :return:                List of actions to perform in batches of 800.
        """
        bulk_json = ""
        bulk_action_list = []

        for i, item in enumerate(content_list, 1):
            id = item["id"]

            if action == "index":
                header = json.dumps({"index": {"_index": self.index, "_id": id}}) + "\n"
                body = json.dumps(item["document"]) + "\n"

            elif action == "update":
                header = json.dumps({"update": {"_index": self.index, "_id": id}}) + "\n"
                body = json.dumps({"doc": item["document"], "doc_as_upsert": True}) + "\n"

            elif action == "delete":
                header = json.dumps({"delete": {"_index": self.index, "_id": id}}) + "\n"
                body = ""

            elif action == "search":
                header = json.dumps({"index": self.index}) + "\n"
                body = json.dumps(item["query"]) + "\n"

            else:
                raise ValueError("Incorrect action supplied. Must be of either index|update|delete|search")

            bulk_json += header + body

            # Every 800 items create a new bulk request
            if i % 800 == 0:
                bulk_action_list.append(bulk_json)
                bulk_json = ""

        # Clean up any remaining jobs
        if bulk_json:
            bulk_action_list.append(bulk_json)

        return bulk_action_list

    def _process_bulk_action_response(self, action_response, api, process=True):
        """
        Process the bulk action response and generate a consilated report of actions
        :param action_response: Response from elasticseach bulk api call
        :param api:             Whether api used was bulk or msearch
        :param process:         True: return consolidated response. False: Return raw response
        :return: Consolidated report | Raw response based on process flag.
        """

        # Return raw response
        if not process:
            return action_response

        if api == "bulk":
            success = 0
            failed = 0
            items_failed = []

            for action in action_response:
                # If there are no errors in the high level json. All items succeeded
                if not action["errors"]:
                    success += len(action["items"])

                else:
                    # Some or all items failed
                    for item in action["items"]:
                        action_key = self._get_action_key(item)

                        # If 2xx HTTP response. Successful
                        if 200 <= item[action_key]["status"] < 300:
                            success += 1

                        else:
                            failed += 1

                            id = item[action_key]["_id"]
                            status = item[action_key]["status"]
                            error = item[action_key]["error"]

                            items_failed.append({
                                "id": id,
                                "status": status,
                                "error": error
                            })

            return {"success": success, "failed": failed, "failed_items": items_failed}

        elif api == "msearch":

            msearch_action_response = []
            for action in action_response:
                response_hits = []

                for response in action["responses"]:
                    response_hits.append(response["hits"]["hits"])

                msearch_action_response.append(response_hits)

            return msearch_action_response


        else:
            raise ValueError("Invalid api selected. Must be of either bulk|msearch")

    def _create_id(self, string):
        return hashlib.sha1(string).hexdigest()

    def _add_item(self, id, doc):
        """
        Update a single document
        :param id: Dictionary containing document body and id in form
        {'document':{},'id':<sha1 hash of filepath>}
        """
        document = {
            'doc': doc,
            'doc_as_upsert': True
        }

        self.es.update(index=self.index, id=id, body=document)