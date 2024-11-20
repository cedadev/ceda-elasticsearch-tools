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
import warnings

CA_ROOT = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), '../root_certificate/root-ca.pem')
        )


class CEDAElasticsearchClient(Elasticsearch):
    """
    Wrapper class to handle SSL authentication with the correct root
    certificate for the cluster. This subclass provides defaults for kwargs from
    the main Elasticsearch Python client.
    
    For read use cases, where the indices of interest are publicly available, it will be sufficient to call:
    
    es = CEDAElasticsearchClient()
    
    For application access, which requires write permissions, you will need to provide an API key. This can be done:
    
    es =  CEDAElasticsearchClient(headers={'x-api-key':'YOUR-API-KEY'})
    
    For further customizations, see the Python Elasticsearch client documentation.
    """

    def __init__(self, hosts=['es%s.ceda.ac.uk:9200' % i for i in range(1,9)], use_ssl=True, ca_certs=CA_ROOT, **kwargs): # ca_certs=None, headers=None, verify_certs=True, scheme="https", **kwargs
        """
        Return elasticsearch client object but always use SSL and
        provide the cluster root certificate
        :param hosts: List of hosts to connect to. Default: [f'es{i}.ceda.ac.uk:9200' for i in range(9,17)]
        :param use_ssl: Default: True
        :param ca_certs: Certificate authority root certificates. Default: CEDA Cluster root certs (set None to disable)
        :param kwargs:
        """
        prefix = "https://" if use_ssl else "http://"

        # Add scheme to hosts if not included
        hosts = [f"{prefix}{host.split('://', 1)[1]}" if host.startswith(("http://", "https://")) else f"{prefix}{host}" for host in hosts]

        print("#\n" * 5)
        print(hosts)
        print("#\n" * 5)


        super(CEDAElasticsearchClient, self).__init__(
            hosts=hosts,
            ca_certs=ca_certs,
            **kwargs
        )