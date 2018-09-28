from elasticsearch import Elasticsearch
import json


class IndexUpdaterBase():
    """
    Base class for index updaters. Contains common methods.
    """

    def __init__(self, index, host, port):
        """
        Common variables.
        :param index:   Index to update
        :param host:    Elasticsearch cluster master host
        :param port:    Elasticsearch cluster port
        """
        self.index = index
        self.es = Elasticsearch(hosts=[{"host": host, "port": port}])

    def _get_action_key(self, es_response_item):
        """
        Get the action key for processing the response
        :param es_response_item:
        :return: key
        """

        actions = ["update", "index", "delete"]
        response_keys = es_response_item.keys()

        return list(set(actions) & set(response_keys))[0]

    def _bulk_action(self, action_list, api="bulk"):
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
        for action in action_list:

            if api == "bulk":
                response = self.es.bulk(index=self.index, body=action)
            elif api == "msearch":
                response = self.es.msearch(body=action)
                print (response)
            else:
                raise ValueError("Invalid api selected. Must be of either bulk|msearch")

            response_list.append(response)

        return self._process_bulk_action_response(response_list, api)

    def _generate_bulk_operation_body(self, content_list, type, action="index"):
        """
        Generate the query body for the bulk operation

        :param content_list:    List of dictionaries containing the content to be actioned upon
        :param action:          The elasticsearch action to perform. (index[default], update, delete)
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

    def _process_bulk_action_response(self, action_response, api):
        """
        Process the bulk action response and generate a consilated report of actions
        :param action_response: Response from elasticseach bulk api call
        :return: Consolidated report
        """

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
                        print (item)
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


class CedaDirs(IndexUpdaterBase):
    """
    Class to aide in updating and managing the ceda-dirs index
    """

    def update_title(self):
        """
        MOLES title has been changed. Update titles for all records matching url

        :return: Status code
        """
        pass

    def update_path(self):
        """
        MOLES record path has been changed. Update directories to have correct information.

        :return: Status code
        """
        pass

    def update_dataset(self):
        """
        A new dataset has been created in MOLES. Add dataset information to relevant directories

        :return: Status code
        """
        pass

    def update_collection(self):
        """
        A new collection has been created in MOLES. Add collection information to relevant directories

        :return: Status code
        """
        pass

    def update_readmes(self, readme_content):
        """
        A new 00README has been added to the archive. Update directories with new content.
        Will fail if the directory does not already exist in the index.

        :return: Status - Success|Failed
        """

        # Generate action list
        update_list = self._generate_bulk_operation_body(readme_content, type="dir", action="update")

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
        bulk_operations = self._generate_bulk_operation_body(directories, type="dir")

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
        bulk_operations = self._generate_bulk_operation_body(directories, type="dir", action='delete')

        # Perform bulk action
        return self._bulk_action(bulk_operations)


class CedaFbi(IndexUpdaterBase):

    def add_files(self):
        pass

    def delete_files(self, files):
        """
        Files have been removed from the file system and need to be removed from the index

        :return:
        """

        # Generate action list
        bulk_operations = self._generate_bulk_operation_body(files, type="file", action='delete')

        # Perform bulk action
        return self._bulk_action(bulk_operations)

    def check_files_existence(self, file_list):
        """
        Wrapper class to underlying operations. Takes a file list and returns a
        list containing the JSON response formatted as below:

        [           # Container for the reponse
            [       # Collection of all the responses in a block as submitted to elasticsearch
                []  # Indiviual query responses
            ]
        ]


        :param file_list:   List of files to test
        :return: List of JSON response objects

        """

        bulk_info = []
        for file in file_list:
            pass



        bulk_operations = self._generate_bulk_operation_body(bulk_info, type="file", action="search")

        return self._bulk_action(bulk_operations, api="msearch")
