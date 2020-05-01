# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '07 Jun 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.index_tools.base import IndexUpdaterBase
import hashlib
import os
import requests
from time import sleep


class CedaDirs(IndexUpdaterBase):
    """
    Class to aide in updating and managing the ceda-dirs index. Includes hooks to allow MOLES
    to trigger changes in the directory index.
    """

    moles_metadata_mapping = {}
    type = "dir"

    def __init__(self, index="ceda-dirs", **kwargs):
        super().__init__(index, **kwargs)

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
        url = "http://api.catalogue.ceda.ac.uk/api/v0/obs/get_info"

        r = requests.get(url + path)
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

        bulk_operations = self._generate_bulk_operation_body(meta_list, action="update")

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

        bulk_operations = self._generate_bulk_operation_body(meta_list, action="update")

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

        bulk_operations = self._generate_bulk_operation_body(meta_list, action="update")

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
        update_list = self._generate_bulk_operation_body(readme_content, action="update")

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
        bulk_operations = self._generate_bulk_operation_body(directories)

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
        bulk_operations = self._generate_bulk_operation_body(directories, action='delete')

        # Perform bulk action
        return self._bulk_action(bulk_operations)

    def add_dir(self, id, doc):
        """
        Convenience method to upsert a single document
        :param id: Document ID (string)
        :param doc: The document to upload (dict)
        """

        self._add_item(id, doc)

    def delete_dir(self, id):
        """
        Delete a single document
        :param id: sha1 hash of filepath
        """

        self.es.delete(index=self.index, id=id)
