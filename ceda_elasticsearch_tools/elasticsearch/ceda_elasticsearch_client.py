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

CA_ROOT = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), '../root_certificate/root-ca.pem')
        )


class CEDAElasticsearchClient(Elasticsearch):
    """
    Wrapper class to handle SSL authentication with the correct root
    certificate for the cluster
    """

    def __init__(self,hosts=[f'es{i}.ceda.ac.uk:9200' for i in range(9,17)], use_ssl=True, ca_certs=CA_ROOT, **kwargs):
        """
        Return elasticsearch client object but always use SSL and
        provide the cluster root certificate
        :param hosts: List of hosts to connect to. Default: [f'es{i}.ceda.ac.uk:9200' for i in range(9,17)]
        :param use_ssl: Default: True
        :param ca_certs: Certificate authority root certificates. Default: CEDA Cluster root certs (set None to disable)
        :param kwargs:
        """

        super().__init__(
            hosts=hosts,
            use_ssl=use_ssl,
            ca_certs=ca_certs,
            **kwargs
        )
