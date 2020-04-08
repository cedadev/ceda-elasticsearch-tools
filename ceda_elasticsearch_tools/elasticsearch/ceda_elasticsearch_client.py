# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '17 Jan 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from elasticsearch import Elasticsearch
import os


class CEDAElasticsearchClient(Elasticsearch):
    """
    Wrapper class to handle SSL authentication with the correct root
    certificate for the cluster
    """

    def __init__(self, **kwargs):
        """
        Return elasticsearch client object but always use SSL and
        provide the cluster root certificate
        :param kwargs:
        """

        ca_root = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), '../root_certificate/root-ca.pem')
        )

        super().__init__(
            [f'es{i}.ceda.ac.uk' for i in range(9,17)],
            use_ssl=True,
            ca_certs=ca_root,
            port=9200,
            verify_certs=False,
            **kwargs
        )
