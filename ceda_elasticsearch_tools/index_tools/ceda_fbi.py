# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '07 Jun 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import hashlib
from ceda_elasticsearch_tools.index_tools.base import IndexUpdaterBase


class CedaFbi(IndexUpdaterBase):
    """
    Class to aide in updating and managing the ceda-fbi index.

    Usage:

    This setup will connect to CEDAs Elasticsearch cluster in a round
    robin. Adding the API key will also allow the actions associated with
    the API key.

    fbi = CedaFbi(
        headers = {
            'x-api-key': 'YOUR-API-KEY'
        }
    )
    """

    type = "file"

    def __init__(self, index="ceda-fbi", **kwargs):
        super().__init__(index, **kwargs)

    def add_file(self, id, doc):
        """
        Convenience method to upsert a single document
        :param id: Document ID (string)
        :param doc: The document to upload (dict)
        """

        self._add_item(id, doc)


    def delete_file(self, document_id):
        """
        Delete a single document by ID
        :param document_id: sha1 hash of the filepath
        """
        self.es.delete(index=self.index, id=document_id)

    def add_files(self, files):
        """
        Add multiple files
        :param files:
        :return:
        """

        # Generate action list
        bulk_operations = self._generate_bulk_operation_body(files, action='update')

        # Perform bulk action
        return self._bulk_action(bulk_operations)

    def delete_files(self, files):
        """
        Files have been removed from the file system and need to be removed from the index

        :return:
        """

        # Generate action list
        bulk_operations = self._generate_bulk_operation_body(files, action='delete')

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

        bulk_operations = self._generate_bulk_operation_body(bulk_request_data, action="search")

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

        bulk_operations = self._generate_bulk_operation_body(bulk_request_data, action="search")

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

        bulk_operations = self._generate_bulk_operation_body(bulk_request_data, action="update")

        return self._bulk_action(bulk_operations)