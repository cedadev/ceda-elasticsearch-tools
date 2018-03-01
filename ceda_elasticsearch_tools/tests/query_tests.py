"""
Test queries for elasticsearch
"""
import unittest
import requests
import simplejson as json
from bs4 import BeautifulSoup

import testQueries as tq

# ELASTIC_URL = "http://jasmin-es1.ceda.ac.uk:9200"
ELASTIC_URL = "http://jasmin-es-test.ceda.ac.uk:9200"


class TestClusterHealth(unittest.TestCase):
    api = "/_cluster/health"

    def setUp(self):
        self.r = requests.get(ELASTIC_URL + self.api)
        response = self.r.json()

        self.cluster = response["cluster_name"]
        self.status = response["status"]
        self.nodeTotal = response["number_of_nodes"]
        self.dataNodeTotal = response["number_of_data_nodes"]
        self.shards = response["active_shards"]
        self.primaryShards = response["active_primary_shards"]
        self.relocatedShards = response["relocating_shards"]
        self.initialisingShards = response["initializing_shards"]
        self.unassignedShards = response["unassigned_shards"]
        self.pendingTasks = response["number_of_pending_tasks"]
        self.maxWaitTime = response["task_max_waiting_in_queue_millis"]
        self.activeShardsPercent = response["active_shards_percent_as_number"]

    def test200(self):
        """
        HTTP request should respond with 200
        """
        self.assertEqual(self.r.status_code, 200, 'Cluster did not respond')

    def testStatus(self):
        """
        Healthy cluster should return Green
        """
        self.assertEqual(self.status, 'green')

    def testNodeCount(self):
        """
        Cluster should register 8 nodes
        """
        self.assertEqual(self.nodeTotal, 8)

    def testActiveShartPercentage(self):
        """
        Healthy cluster should register 100%
        """
        self.assertEqual(self.activeShardsPercent, 100.0)


class TestMainAliases(unittest.TestCase):
    api = "/_cat/aliases"

    def setUp(self):
        self.r = requests.get(ELASTIC_URL + self.api)
        self.alias_list = self.r.text.split()[0::5]

    def test200(self):
        """
        HTTP request should respond with 200
        """
        self.assertEqual(self.r.status_code, 200, 'Cluster did not respond')

    def testAliasCedaeo(self):
        """
        ceda-eo alias present
        """
        self.assertIn('ceda-eo', self.alias_list)

    def testAliasArsf(self):
        """
        arsf alias present
        """
        self.assertIn('arsf', self.alias_list)

    def testAliasEufar(self):
        """
        eufar alias present
        """
        self.assertIn('eufar', self.alias_list)

    def testAliasFaam(self):
        """
        faam alias present
        """
        self.assertIn('faam', self.alias_list)

    def testAliasCedaMolesHaystack(self):
        """
        cedamoles-haystack-prod alias present
        """
        self.assertIn('cedamoles-haystack-prod', self.alias_list)

    def testAliasLeve1(self):
        """
        ceda-level-1 alias present
        """
        self.assertIn('ceda-level-1', self.alias_list)

    def testAliasLevel2(self):
        """
        ceda-level-2 alias present
        """
        self.assertIn('ceda-level-2', self.alias_list)

    def testAliasLevel3(self):
        """
        ceda-level-3 alias present
        """
        self.assertIn('ceda-level-3', self.alias_list)


class TestEOQueries(unittest.TestCase):
    api = "/ceda-eo/_search"

    def testBaseQuery(self):
        """
        Just checking that we return results
        """
        r = requests.get(ELASTIC_URL + self.api, data=json.dumps(tq.CedaEOQueries().basic()))
        self.assertEqual(r.status_code, 200)
        self.assertGreater(r.json()["hits"]["total"], 0)

    def testGeoShapeQuery(self):
        """
        Just checking that we return results
        """
        r = requests.get(ELASTIC_URL + self.api, data=json.dumps(tq.CedaEOQueries().geo_shape()))
        self.assertEqual(r.status_code, 200)
        self.assertGreater(r.json()["hits"]["total"], 0)

    def testAggs(self):
        """
        Checking return 8 satellite missions:
            Sentinel-1A
            Sentinel-1B
            Sentinel-2A
            Sentinel-2B
            Sentinel-3
            Landsat-5
            Landsat-7
            Landsat-8

        """
        r = requests.get(ELASTIC_URL + self.api, data=json.dumps(tq.CedaEOQueries().aggs()))
        self.assertEqual(r.status_code, 200)

        buckets = r.json()["aggregations"]["all"]["satellites"]["buckets"]

        self.assertEqual(len(buckets), 8)

    def testAllDocsHaveMission(self):
        """
        Makes sure that all records contain Mission parameter. Indicates some contamination of eufar/famm/arsf docs. If
        index returns docs with missing mission.
        """
        r = requests.get(ELASTIC_URL + self.api, data=json.dumps(tq.CedaEOQueries().haveMission()))
        self.assertEqual(r.status_code, 200)

        self.assertEqual(r.json()["hits"]["total"], 0)


class TestEFFQueries(unittest.TestCase):

    def search_api(self, index):
        """
        Return the api extention for specified index
        """
        return "/{index}/_search".format(index=index)

    def testARSFresponds(self):
        """
        Check that the arsf index responds with data
        """
        r = requests.get(ELASTIC_URL + self.search_api("arsf"))

        self.assertGreater(r.json()["hits"]["total"], 0)

    def testFAAMresponds(self):
        """
        Check that the faam index responds with data
        """
        r = requests.get(ELASTIC_URL + self.search_api("faam"))

        self.assertGreater(r.json()["hits"]["total"], 0)

    def testEUFARresponds(self):
        """
        Check that the eufar index responds with data
        """
        r = requests.get(ELASTIC_URL + self.search_api("eufar"))

        self.assertGreater(r.json()["hits"]["total"], 0)


class TestOpensearchQueries(unittest.TestCase):
    def setUp(self):
        """
        Download opensearch status page
        """
        r = requests.get("http://opensearch.ceda.ac.uk/status/")
        self.soup = BeautifulSoup(r.text, 'html.parser')

    def testStatusQueries(self):
        """
        Check that all queries on the page return success and none have failed.
        """
        for row in self.soup.tbody.find_all('tr'):
            self.assertIn('bg-success', row['class'], msg="Check opensearch.ceda.ac.uk/status/ for details")


class TestIndexMapping(unittest.TestCase):
    test_index = "/richard-unittest-mapping-index"

    def testCEDADIMapping(self):
        """
        Check that the index accepts the mapping supplied to it from ceda-di
        """
        mapping = tq.IndexMappings.cedadiMapping()
        r = requests.put(url=ELASTIC_URL + self.test_index, data=json.dumps(mapping), headers={"Content-Type": "application/json"})
        if r.status_code != 200:
            print r.json()

        self.assertEqual(r.status_code,200)

    def testFBSMapping(self):
        """
        Check that the index accepts the mapping supplied to it from FBS
        """
        mapping = tq.IndexMappings.fbsMapping()
        r = requests.put(url=ELASTIC_URL + self.test_index, data=json.dumps({}), headers={"Content-Type": "application/json"})

        self.assertEqual(r.status_code, 200)

    def tearDown(self):
        requests.delete(url=ELASTIC_URL + self.test_index)


if __name__ == '__main__':
    index = sys.argv[1]
    unittest.main()
