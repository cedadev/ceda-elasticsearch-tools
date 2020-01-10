# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '07 Jun 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.index_tools.base import IndexUpdaterBase


class CedaEo(IndexUpdaterBase):
    type = "geo_metadata"

    def __init__(self, host_urls, index="ceda-eo", **kwargs):
        super(CedaEo, self).__init__(index, host_urls, **kwargs)

    def update_file_location(self, file_list, on_disk=True):
        # set the location value
        location = "on_disk" if on_disk else "on_tape"

        # Generator to create request information
        bulk_request_data = (
            {
                "id": self._create_id(file),
                "document": {"file": {"location": location}}
            } for file in file_list
        )

        bulk_operations = self._generate_bulk_operation_body(bulk_request_data, action="update")

        return self._bulk_action(bulk_operations)
