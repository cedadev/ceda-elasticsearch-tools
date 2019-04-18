from elasticsearch import Elasticsearch
import json
import hashlib
from tqdm import tqdm
import os
import requests
from time import sleep
import sys


class IndexUpdaterBase(object):
    """
    Base class for index updaters. Contains common methods.
    """

    def __init__(self, index, host_url, **kwargs):
        """
        Common variables.
        :param index:   Index to update
        :param host:    Elasticsearch cluster master host
        :param port:    Elasticsearch cluster port
        """
        self.index = index
        self.es = Elasticsearch(hosts=[host_url], **kwargs)

    def _get_action_key(self, es_response_item):
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
        :return:        List of results
        """

        all_results = []

        # Get first search
        page = self.es.search(
            index=self.index,
            scroll='1m',
            size=1000,
            body=query)

        sid = page['_scroll_id']
        total_results = page['hits']['total']
        all_results.extend(page['hits']['hits'])

        # Floor division
        scroll_count = total_results // size

        # Start scrolling
        for i in range(scroll_count):
            page = self.es.scroll(scroll_id=sid, scroll='1m')

            # Update the scroll ID
            sid = page['_scroll_id']

            # Add results to main results list
            hits = page['hits']['hits']
            all_results.extend(hits)

        # Clear the scroll context to free memory
        self.es.clear_scroll(scroll_id=sid)

        return all_results

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

    def _generate_bulk_operation_body(self, content_list, type, action="index"):
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
                header = json.dumps({"index": {"_index": self.index, "_type": type, "_id": id}}) + "\n"
                body = json.dumps(item["document"]) + "\n"

            elif action == "update":
                header = json.dumps({"update": {"_index": self.index, "_type": type, "_id": id}}) + "\n"
                body = json.dumps({"doc": item["document"]}) + "\n"

            elif action == "delete":
                header = json.dumps({"delete": {"_index": self.index, "_type": type, "_id": id}}) + "\n"
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


class CedaDirs(IndexUpdaterBase):
    """
    Class to aide in updating and managing the ceda-dirs index. Includes hooks to allow MOLES
    to trigger changes in the directory index.
    """

    moles_metadata_mapping = {}
    type = "dir"

    def __init__(self, host_url, index="ceda-dirs", **kwargs):
        super(CedaDirs, self).__init__(index, host_url, **kwargs)

    def _get_moles_metadata(self, path):
        """
        Get MOLES metadata for a given path.
        Caches results in an object level dict to try and reduce calls the to API

        :param path:    Path to test
        :return:        JSON object (dict)
        """
        # TODO: Switch this to use new JSON response when it is live
        # Recursively check for a match in cached mapping
        # dir = path
        # while len(dir) > 1:
        #     if dir in self.moles_metadata_mapping:
        #         return self.moles_metadata_mapping[dir]
        #     elif dir + "/" in self.moles_metadata_mapping:
        #         return self.moles_metadata_mapping[dir + "/"]
        #     else:
        #         dir = os.path.dirname(dir)

        # If no match found check MOLES API for metadata
        url = "http://catalogue-test.ceda.ac.uk/api/v0/obs/get_info"

        r = requests.get(url + path)
        # print (r.text)
        r_json = r.json()

        # If there is a response from the API add that to the dict to reduce calls in the future
        if r_json['title']:
            self.moles_metadata_mapping[path] = r_json
            return r_json

    def _backfill_moles_meta(self, dir_list):
        """
        Take a list of directories and generates their metadata using the MOLES api.

        Process::

            1. Sort list of directories by length to try and go from bottom to top
            2. Check metadata dictionary to see if there is already a match
            3. If not, poll MOLES API and retrieve metadata

        :param dir_list: List of directories to get metadata for
        :return: list of metadata <dict> objects for each directory
        """

        # Sort directory on length to backfill from bottom up and reduce the number of moles queries
        sorted_dir_list = sorted(dir_list, key=len, reverse=True)

        meta_list = []
        for dir in sorted_dir_list:
            metadata = self._get_moles_metadata(dir)
            if metadata and metadata['title']:
                document = {
                    'title': metadata['title'],
                    'url': metadata['url'],
                    'record_type': metadata['record_type'].lower()
                }

                update_object = {
                    "id": hashlib.sha1(dir).hexdigest(),
                    "document": document

                }

                meta_list.append(update_object)

        return meta_list

    def backfill_above_path(self, path):
        """
        Backfill MOLES collection information above given path.

        :param path: Path to back fill metadata from
        """

        recursion_list = []

        dir = os.path.dirname(path)
        while len(dir) > 1:
            recursion_list.append(dir)
            dirname = os.path.dirname(dir)
            dir = dirname

        recursion_list.sort(key=lambda s: len(s))

        meta_list = self._backfill_moles_meta(recursion_list)

        bulk_operations = self._generate_bulk_operation_body(meta_list, type=self.type, action="update")

        self._bulk_action(bulk_operations)

        return

    def update_title(self, url, title):
        """
        MOLES title has been changed. Update titles for all records matching url.

        Process::

            1. Update by query based on url

        :param url:     MOLES record URL
        :param title:   New title to apply
        """

        query = {
            "query": {
                "term": {
                    "url.keyword": {
                        "value": url
                    }
                }
            },
            "script": "ctx._source.title = \"{}\";".format(title)
        }

        self.es.update_by_query(index=self.index, body=query)

        return

    def update_path(self, url, new_path, title, record_type="dataset"):
        """
        MOLES record path has been changed. Update directories to have correct information.

        Process::

            1. Get all matching records by url
            2. Get all matching records by new path
            3. Diff the results to get back_fill list
            4. Clear MOLES information for all matching records
            5. Apply MOLES information to all records matching paths
            6. Back fill after change:

                - Take difference between queries
                - Poll MOLES API to get dataset/collection info
                - Apply to reduced set of files
            7. Back fill above new location

        :param url:             URL for the dataset/collection
        :param new_path:        New path to apply metadata to
        :param title:           Title of the MOLES record
        :param record_type:     MOLES record type. (default: dataset)
        """
        # Get all matching records by URL
        query = {
            "_source": "archive_path",
            "query": {
                "term": {
                    "url.keyword": {
                        "value": url
                    }
                }
            }
        }

        url_matches = self._scroll_search(query=query)

        url_matches = [match['_source']['archive_path'] for match in url_matches]

        # Get all matching records by new path
        query = {
            "_source": "archive_path",
            "query": {
                "match_phrase_prefix": {
                    "archive_path": new_path
                }
            }
        }

        path_matches = self._scroll_search(query=query)

        path_matches = [match['_source']['archive_path'] for match in path_matches]

        # Get diff list via (URL matches) - (Path Matches) to get all files which will not be updated by the new path
        backfill_list = set(url_matches) - set(path_matches)

        # Clear MOLES information for all matching records
        query = {
            "query": {
                "term": {
                    "url.keyword": {
                        "value": url
                    }
                }
            },
            "script": "ctx._source.remove('title');ctx._source.remove('url');ctx._source.remove('record_type');"
        }

        self.es.update_by_query(index=self.index, body=query)

        # wait for shards to catch up
        sleep(1)

        # Apply MOLES information to all records matching paths

        query = {
            "query": {
                "match_phrase_prefix": {
                    "archive_path": new_path
                }
            },
            "script": "ctx._source.url = '{url}';ctx._source.title = '{title}';ctx._source.record_type = '{record_type}';".format(
                url=url, title=title, record_type=record_type)
        }

        self.es.update_by_query(index=self.index, body=query)

        # Back fill after change
        meta_list = self._backfill_moles_meta(backfill_list)

        bulk_operations = self._generate_bulk_operation_body(meta_list, type=self.type, action="update")

        self._bulk_action(bulk_operations)

        # Recurse up new path and poll MOLES for changes
        self.backfill_above_path(new_path)

        return

    def update_dataset(self, url, path, title, record_type="dataset"):
        """
        A new dataset has been created in MOLES. Add dataset information to relevant directories.

        Process::

            1. Update by query below path
            2. Back fill above new location

        :param url:         MOLES URL to add to matches
        :param path:        Directory path to update dataset information below
        :param title:       MOLES record title to add to matches
        :param record_type: MOLES record type. (default: dataset)
        """

        # Update by query below path

        query = {
            "query": {
                "match_phrase_prefix": {
                    "path": path
                }
            },
            "script": "ctx._source.url = '{url}';ctx._source.title = '{title}';ctx._source.record_type = '{record_type}';".format(
                url=url, title=title, record_type=record_type)
        }

        self.es.update_by_query(index=self.index, body=query)

        # Backfill above new location

        self.backfill_above_path(path)

    def update_collection(self, paths):
        """
        A new collection has been created in MOLES. Add collection information to relevant directories.

        Process::

        1. Back fill paths with collection

        :param paths: List of paths to update

        """

        meta_list = self._backfill_moles_meta(paths)

        bulk_operations = self._generate_bulk_operation_body(meta_list, type=self.type, action="update")

        self._bulk_action(bulk_operations)

    def delete_catalogue_record(self, url):
        """
        Delete MOLES information for records matching url

        :param url: URL of deleted MOLES record
        """

        query = {
            "query": {
                "term": {
                    "url.keyword": {
                        "value": url
                    }
                }
            }
        }

        self.es.delete_by_query(index=self.index, body=query)

        return

    def update_readmes(self, readme_content):
        """
        A new 00README has been added to the archive. Update directories with new content.
        Will fail if the directory does not already exist in the index.

        :return: Status - Success|Failed
        """

        # Generate action list
        update_list = self._generate_bulk_operation_body(readme_content, type=self.type, action="update")

        # Perform bulk action
        return self._bulk_action(update_list)

    def add_dirs(self, directories):
        """
        New directories have been added to the archive. Add directories to directories index

        :param directories: List of dictionary items containing information to be processed in request. Path key is required.
                            eg. [{"path": "/neodc/sentinel1b/data/IW/L1_SLC/IPF_v2/2018/09, "record_type": "Dataset"}, ...]

        :return: Status - Success|Failed
        """

        # Generate action list
        bulk_operations = self._generate_bulk_operation_body(directories, type=self.type)

        # Perform bulk action
        return self._bulk_action(bulk_operations)

    def delete_dirs(self, directories):
        """
        Directories have been deleted from the archive. Remove directories from the index.

        :param directories:  List of dictionary items containing information to be processed in request. Path key is required.
                            eg. [{"path": "/neodc/sentinel1b/data/IW/L1_SLC/IPF_v2/2018/09"}, ...]

        :return: Status - Success|Failed
        """

        # Generate action list
        bulk_operations = self._generate_bulk_operation_body(directories, type=self.type, action='delete')

        # Perform bulk action
        return self._bulk_action(bulk_operations)


class CedaFbi(IndexUpdaterBase):
    """
    Class to aide in updating and managing the ceda-fbi index.
    """

    type = "file"

    def __init__(self, host_url, index="ceda-fbi", **kwargs):
        super(CedaFbi, self).__init__(index, host_url, **kwargs)

    def add_files(self, files):

        # Generate action list
        bulk_operations = self._generate_bulk_operation_body(files, type=self.type)

        # Perform bulk action
        return self._bulk_action(bulk_operations)

    def delete_files(self, files):
        """
        Files have been removed from the file system and need to be removed from the index

        :return:
        """

        # Generate action list
        bulk_operations = self._generate_bulk_operation_body(files, type=self.type, action='delete')

        # Perform bulk action
        return self._bulk_action(bulk_operations)

    def check_dir_count(self, dir_list):
        """
        Takes a list of directories and counts the number of hits in each directory.
        Returned as raw elasticsearch response::

            [
                {"responses":[
                    {
                        "hits:[],
                        "aggregations":{
                            "file_count":{
                                "value":<count>}
                            }
                        }
                    }
                ]},...
            ]

        :param dir_list: List of directories
        :return:
        """

        bulk_request_data = []

        # Dont need id but it is required for _generate_bulk_operation_body func
        id = ""

        for dir in dir_list:
            bulk_request_data.append(
                {
                    "id": id,
                    "query": {
                        "query": {
                            "term": {
                                "info.directory": {
                                    "value": dir
                                }
                            }
                        },
                        "aggs": {
                            "file_count": {
                                "value_count": {
                                    "field": "info.directory"
                                }
                            }
                        },
                        "size": 0
                    }
                }
            )

        bulk_operations = self._generate_bulk_operation_body(bulk_request_data, type=self.type, action="search")

        return self._bulk_action(bulk_operations, api="msearch", process_results=False)

    def check_files_existence(self, file_list):
        """
        Wrapper class to underlying operations. Takes a file list and returns a
        list containing the JSON response formatted as below::

            [           # Container for the reponse
                [       # Collection of all the responses in a block as submitted to elasticsearch
                    []  # Indiviual query responses
                ]
            ]

        :param file_list:   List of files to test
        :return: List of JSON response objects
        """

        bulk_request_data = []
        for file in file_list:
            id = hashlib.sha1(file).hexdigest()
            bulk_request_data.append(
                {
                    "id": id,
                    "query": {"query": {"term": {"_id": {"value": id}}}}
                }
            )

        bulk_operations = self._generate_bulk_operation_body(bulk_request_data, type=self.type, action="search")

        return self._bulk_action(bulk_operations, api="msearch")

    def update_file_location(self, file_list, on_disk=True):
        """
        Update location of file on_disk or on_tape
        :param file_list: List of files to change
        :param on_disk: Boolean, status. True will set location to on_disk False, on_tape
        :return:
        """

        # set the location value
        location = "on_disk" if on_disk else "on_tape"

        # Generator to create request information
        bulk_request_data = (
            {
                "id": self._create_id(file),
                "document": {"info": {"location": location}}
            } for file in file_list
        )

        bulk_operations = self._generate_bulk_operation_body(bulk_request_data, type=self.type, action="update")

        return self._bulk_action(bulk_operations)


class CedaEo(IndexUpdaterBase):
    type = "geo_metadata"

    def __init__(self, host_url, index="ceda-eo", **kwargs):
        super(CedaEo, self).__init__(index, host_url, **kwargs)

    def update_file_location(self, file_list, on_disk=True):
        # set the location value
        location = "on_disk" if on_disk else "on_tape"

        # Generator to create request information
        bulk_request_data = (
            {
                "id": self._create_id(file),
                "document": {"file": {"location": location}}
            } for file in file_list
        )

        bulk_operations = self._generate_bulk_operation_body(bulk_request_data, type=self.type, action="update")

        return self._bulk_action(bulk_operations)
