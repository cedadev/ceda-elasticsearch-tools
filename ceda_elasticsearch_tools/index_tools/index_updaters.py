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

    def _bulk_action(self, action_list):
        """
        Perform bulk action to elasticsearch

        :param action_list: List of bulk index operations.

        """

        for action in action_list:
            return self.es.bulk(index=self.index, body=action)


    def _generate_bulk_operation_body(self, content_list, type, action="index"):
        """
        Generate the query body for the bulk operation

        :param content_list:    List of dictionaries containing the content to be actioned upon
        :param action:          The elasticsearch action to perform. (index[default], update, delete)
        :return:                List of actions to perform in batches of 800.
        """

        bulk_json = ""
        bulk_action_list = []

        for i, item in enumerate(content_list,1):
            # path = item['path']
            # id = hashlib.sha1(path).hexdigest()
            id = item["id"]

            if action == "index":
                header = json.dumps({"index": {"_index": self.index, "_type": type, "_id": id}}) + "\n"
                body = json.dumps(item["document"]) + "\n"

            elif action == "update":
                header = json.dumps({"update": {"_index": self.index, "_type": type, "_id": id}}) + "\n"
                body = json.dumps({"doc": item["document"]}) + "\n"

            elif action == "delete":
                header = json.dumps({"delete" : {"_index": self.index, "_type": type, "_id": id}}) + "\n"
                body=""

            bulk_json += header + body

            # Every 800 items create a new bulk request
            if i % 800 == 0:
                bulk_action_list.append(bulk_json)
                bulk_json = ""

        # Clean up any remaining jobs
        if bulk_json:
            bulk_action_list.append(bulk_json)

        return bulk_action_list

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
        errors = self._bulk_action(update_list)

        if not errors:
            return "Success"
        else:
            return "Failed"

    def add_dirs(self, directories):
        """
        New directories have been added to the archive. Add directories to directories index

        :param directories: List of dictionary items containing information to be processed in request. Path key is required.
                            eg. [{"path": "/neodc/sentinel1b/data/IW/L1_SLC/IPF_v2/2018/09, "record_type": "Dataset"}, ...]

        :return: Status - Success|Failed
        """

        # Generate action list
        bulk_operations =  self._generate_bulk_operation_body(directories, type="dir")

        # Perform bulk action
        errors = self._bulk_action(bulk_operations)

        if not errors:
            return "Success"
        else:
            return "Failed"

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
        errors = self._bulk_action(bulk_operations)

        if not errors:
            return "Success"
        else:
            return "Failed"

