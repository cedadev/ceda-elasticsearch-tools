import unittest
from ceda_elasticsearch_tools.index_tools import index_updaters
import json
from elasticsearch import Elasticsearch
import hashlib
import time


# class TestIndexUpdaterBase(unittest.TestCase):
#     """
#     Test main functionality of base class methods
#     """
#
#     def setUp(self):
#         self.index = "elasticsearch-tools-unittest"
#         self.base = index_updaters.IndexUpdaterBase(index=self.index, host="jasmin-es1.ceda.ac.uk",
#                                                     port=9200)
#
#         # load data
#         with open("data/new_data.json") as reader:
#             self.data = json.load(reader)
#
#         # Process data to right format
#         self.content_list = []
#
#         for doc in self.data['json']:
#             self.content_list.append(
#                 {
#                     'id': hashlib.sha1(doc['path']).hexdigest(),
#                     'document': doc
#                 }
#             )
#
#
#     def test_generate_bulk_action_body(self):
#
#         bulk_operations = self.base._generate_bulk_operation_body(self.content_list, "dir")
#
#         test_data = [
#             '{"index": {"_type": "dir", "_id": "55cf1709c1b70ae1432e8a4c4fcbc1de21a575af", "_index": "elasticsearch-tools-unittest"}}',
#             '{"archive_path": "/badc/specs/data/SPECS/output/MPI-M/MPI-ESM-LR/decadal/S19320101/mon/atmos/Amon/ta/r3i2p1/v20161102", "title": "SPECS - MPI-ESM-LR model output prepared for SPECS decadal (1901-2015)", "url": "http://catalogue.ceda.ac.uk/uuid/72fcd8b56e6d4e468a80cfa01d645d20", "record_type": "dataset", "depth": 15, "link": false, "path": "/badc/specs/data/SPECS/output/MPI-M/MPI-ESM-LR/decadal/S19320101/mon/atmos/Amon/ta/r3i2p1/v20161102", "type": "dir", "dir": "v20161102"}'
#         ]
#
#         self.assertEqual(len(bulk_operations), 1)
#         self.assertEqual(len(bulk_operations[0].split("\n")), 101)
#         self.assertEqual(bulk_operations[0].split("\n")[:2], test_data)
#
#     def test_bulk_action(self):
#
#         bulk_operations = self.base._generate_bulk_operation_body(self.content_list, "dir")
#
#         response = self.base._bulk_action(bulk_operations, process_results=False)
#
#
#         self.assertEqual(response[0]['items'][0]['index']['status'],200)
#         self.assertEqual(self.base.es.count(index=self.base.index)['count'],50)
#
#
#
#     def tearDown(self):
#         """
#         Delete the created index
#         """
#
#         self.base.es.indices.delete(self.index, ignore=[404,400])

class TestCedaDirsUpdaterDeposit(unittest.TestCase):
    """
    Test methods used by the deposit update processes
    """

    def setUp(self):
        """
        Create an index with some items

        :return:
        """

        # # setup base connection
        self.index = "elasticsearch-tools-unittest"
        self.dir_connection = index_updaters.CedaDirs(index=self.index, host="jasmin-es1.ceda.ac.uk", port=9200)

        # Open data file
        with open("data/new_data.json") as reader:
            data = json.load(reader)

        # Process data to right format
        self.content_list = []

        for doc in data['json']:
            self.content_list.append(
                {
                    'id': hashlib.sha1(doc['path']).hexdigest(),
                    'document': doc
                }
            )

        bulk_operations = self.dir_connection._generate_bulk_operation_body(self.content_list, "dir")

        self.dir_connection._bulk_action(bulk_operations)

        time.sleep(1)

    def test_update_title(self):

        url = "http://catalogue.ceda.ac.uk/uuid/72fcd8b56e6d4e468a80cfa01d645d20"
        new_title = "New title for a moles record"

        self.dir_connection.update_title(url=url,
                                         title=new_title)

        time.sleep(1)

        query = {
            "query": {
                "term": {
                    "url.keyword": {
                        "value": url
                    }
                }
            }
        }

        result = self.dir_connection.es.search(index=self.index, body=query)

        self.assertEqual(result['hits']['hits'][0]['_source']['title'], new_title)

    def test_delete_catalogue_record(self):

        url = "http://catalogue.ceda.ac.uk/uuid/72fcd8b56e6d4e468a80cfa01d645d20"

        self.dir_connection.delete_catalogue_record(url)

        time.sleep(1)

        query = {
            "query": {
                "term": {
                    "url.keyword": {
                        "value": url
                    }
                }
            }
        }

        result = self.dir_connection.es.search(index=self.index, body=query)

        self.assertEqual(len(result['hits']['hits']), 0)

    def test_update_dataset(self):

        # Test variables
        archive_path = "/badc/specs/data/SPECS/output/ECMWF/IFS4/soilMoistureInit/"
        url = "unique_url"
        title = "New title"

        self.dir_connection.update_dataset(url=url, path=archive_path, title=title)

        time.sleep(1)

        query = {
            "query": {
                "term": {
                    "url.keyword": {
                        "value": url
                    }
                }
            }
        }

        result = self.dir_connection.es.search(index=self.index, body=query)

        hits = result['hits']['hits']

        self.assertEqual(len(hits), 3)
        for hit in hits:
            response_url = hit['_source']['url']
            response_title = hit['_source']['title']
            response_record_type = hit['_source']['record_type']

            self.assertEqual(response_url, url)
            self.assertEqual(response_title, title)
            self.assertEqual(response_record_type, "dataset")

    def test__backfill_moles_meta(self):

        path_list = [
            "/badc/specs/data/SPECS/output/ECMWF/IFS4/soilMoistureInit/S19870501/day/atmos/day/tasmin/r1i1p1/files"]

        response = self.dir_connection._backfill_moles_meta(path_list)

        self.assertTrue(response[0]['document']['title'])

    def test_update_path(self):

        data = {
            "url": "http://catalogue.ceda.ac.uk/uuid/57f5fd7e369e4467a56f0f227060cd44",
            "new_path": "/badc/specs/data/SPECS/output/CNRM/CNRM-CM5/seaIceInit/S20011101/",
            "title": "Obviously different title"
        }

        self.dir_connection.update_path(**data)

        query = {
            "query": {
                "match_phrase_prefix": {
                    "archive_path": data['new_path']
                }
            }
        }

        result = self.dir_connection.es.search(index=self.index, body=query)

        self.assertEqual(result['hits']['hits'][0]['_source']['title'], data['title'])

    def tearDown(self):
        self.dir_connection.es.indices.delete(index=self.index)


class TestCedaDirsUpdaterMoles(unittest.TestCase):
    """
    Test methods used by the MOLES update process
    """
    pass


class TestCedaFbiUpdaterDeposit(unittest.TestCase):
    """
    Test methods used by the deposit update process
    """
    pass


if __name__ == '__main__':
    unittest.main()
