# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '09 Feb 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import unittest
from unittest.mock import patch

from ceda_elasticsearch_tools.core.log_reader import SpotMapping


def mock_realpath(path):
    mapping = {
        '/badc/cmip5': '/datacentre/archvol5/qb176/archive/cmip5',
        '/neodc/esacci/ghg': '/datacentre/archvol5/qb188/archive/spot-2207-esacci_ghg',
        '/neodc/esacci/ghg/data/crdp_4/GOSAT/CH4_GOS_OCFP/v2.1': '/datacentre/archvol5/qb188/archive/spot-2207-esacci_ghg/data/crdp_4/GOSAT/CH4_GOS_OCFP/v2.1',
    }
    return mapping[path]


class SpotMappingTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spotm = SpotMapping()

    def test_get_spot_from_storage_path(self):
        paths = [
            (
                '/badc/cmip5',
                '/datacentre/archvol5/qb176/archive/cmip5',
                ('cmip5', None)
            ),
            (
                '/neodc/esacci/ghg',
                '/datacentre/archvol5/qb188/archive/spot-2207-esacci_ghg',
                ('spot-2207-esacci_ghg', None)
            ),
            (
                '/neodc/esacci/ghg/data/crdp_4/GOSAT/CH4_GOS_OCFP/v2.1',
                '/datacentre/archvol5/qb188/archive/spot-2207-esacci_ghg/data/crdp_4/GOSAT/CH4_GOS_OCFP/v2.1',
                ('spot-2207-esacci_ghg', 'data/crdp_4/GOSAT/CH4_GOS_OCFP/v2.1')
            )
        ]

        for path, storage_path, expected in paths:
            spot, suffix = self.spotm.get_spot_from_storage_path(storage_path)
            self.assertEqual(spot, expected[0])
            self.assertEqual(suffix, expected[1])

    def test_get_archive_root(self):

        spots = [
            ('cmip5', '/badc/cmip5'),
            ('spot-2207-esacci_ghg', '/neodc/esacci/ghg'),
            ('spot-2207-esacci_ghg', '/neodc/esacci/ghg')
        ]

        for spot, expected in spots:
            spot_path = self.spotm.get_archive_root(spot)
            self.assertEqual(spot_path, expected)

    @patch('os.path.realpath', mock_realpath)
    def test_get_archive_path(self):

        paths = [
            (
                '/badc/cmip5',
                '/badc/cmip5',
            ),
            (
                '/neodc/esacci/ghg',
                '/neodc/esacci/ghg',
            ),
            (
                '/neodc/esacci/ghg/data/crdp_4/GOSAT/CH4_GOS_OCFP/v2.1',
                '/neodc/esacci/ghg/data/crdp_4/GOSAT/CH4_GOS_OCFP/v2.1',
            )
        ]

        for path, expected in paths:
            archive_path = self.spotm.get_archive_path(path)

            self.assertEqual(archive_path, expected)
