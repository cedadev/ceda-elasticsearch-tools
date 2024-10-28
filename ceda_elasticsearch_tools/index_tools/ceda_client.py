# encoding: utf-8

__author__ = 'Daniel Westwood'
__date__ = '20 Jun 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

from .base import IndexUpdaterBase
from elasticsearch.exceptions import AuthenticationException

import json
import os

def gen_id():
    import random
    chars = [*'0123456789abcdefghijklmnopqrstuvwxyz']
    xid = ''
    for i in range(39):
        j = random.randint(0,len(chars)-1)
        xid += chars[j]
    return xid
    # Probability of id reuse is negligible.

class BulkClient(IndexUpdaterBase):

    def __init__(self, index, fieldmatch, es_config=None, **kwargs):
        self.fieldmatch = fieldmatch

        if not os.path.isfile(es_config):
            raise AuthenticationException(f'File {es_config} not present, no settings loaded.')
        with open(es_config) as f:
            connection_kwargs = json.load(f) | kwargs

        super().__init__(index, **connection_kwargs)

        if not self.es.indices.exists(index):
            self.es.indices.create(index)

    def obtain_records(self):
        search = {
            "query": {
                "match_all": {}
            }
        }
        resp = self.es.search(search, index=self.index)

        try:
            return resp['hits']['hits']
        except IndexError:
            return None

    def get_ids(self):
        hits = self.obtain_records()
        ids = {}

        for record in hits:
            value = record['_id']
            key   = record['_source'][self.fieldmatch]
            ids[key] = value
        
        return ids

    def upload(self, action, content_list):
        """
        Upload from the content list given a specific action.
        E.g 'add', 'update' etc.
        """

        content_list = self._preprocess_records(content_list)
        action_list = self._generate_bulk_operation_body(
            content_list,
            action=action
        )
        status = self._bulk_action(action_list)
        print(f"Uploaded content ({action}) ", status)

    def add_records(self, records):
        ids = self.get_ids()
        print(f'Found {len(ids)} existing records')

        update = []
        add = []
        for record in records:
            if record[self.fieldmatch] in ids:
                update.append(
                    {'id': ids[record[self.fieldmatch]], 'document': record}
                )
            else:
                add.append(
                    {'id': gen_id(), 'document': record}
                )

        print(f'Updates: {len(update)}, Additions: {len(add)}')

        if len(update) > 0:
            self.upload('update', update)
        if len(add) > 0:
            self.upload('index', add)

    def _preprocess_records(self, content_list):
        """
        Method to override for inserting perprocessing
        to the list of records
        """
        return content_list
