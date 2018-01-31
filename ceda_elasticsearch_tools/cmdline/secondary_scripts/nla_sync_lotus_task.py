'''
nla_sync_lotus_task.py

Updates the file location on the target elasticsearch index using the NLA as a baseline.
Files which are not visible on disk at the time of scanning will not be in ES. Use the
--index-doc flag to put these missing documents in elasticsearch.
If NLA status ONDISK or RESTORED and scan level is set > 1, create jobs for lotus to add
these files at the desired detail.

To be used as a command line tool.

Usage:
   file_on_tape.py (-i INDEX)
                (-f INPUT_FILE)
                (-o OUTPUT_DIR)
                (--on-tape | --on-disk )
                [(--host HOST)(--port PORT)]
                [--index-docs]
    file_on_tape.py --version

Options:
    -h --help       Show this screen.
    --version       Show version.
    -i              Elasticsearch index.
    -f              Input file containing list of file paths to update
    --on-tape       NLA designates the primary copy of these files as on tape
    --on-disk       NLA designates these files are available on disk. Be that primary copy on disk or restored to disk.
    --host          Elasticsearch host to target (default: jasmin-es1.ceda.ac.uk ).
    --port          Elasticsearch port to target (default: 9200 ).
    --index-docs    Index files not found at level 1 detail or if RESTORED create job for lotus.
'''
from docopt import docopt

from ceda_elasticsearch_tools.core import updater
from ceda_elasticsearch_tools.core.updater import ElasticsearchQuery
import pkg_resources
import os
import simplejson as json
import re
import hashlib
from elasticsearch import Elasticsearch


class NLASync():

    def __init__(self, config):
        self.CONFIG = config
        self.location = None

        if config['--on-disk']:
            self.location = True
        if config['--on-tape']:
            self.location = False

        # Load input data
        with open(config['INPUT_FILE']) as reader:
            self.file_dict = json.load(reader)
            self.file_list = self.file_dict.keys()

        # Regex patterns for file format
        self.regex_patterns = {
            "NetCDF": "(\.nc)$",
            "NASA Ames": "(\.na)$",
            "PP": "(\.pp)$",
            "GRIB": "(\.grb)$|(\.grib)$|(\.GRB)$|(\.GRIB)$  ",
            "Manifest": "(\.manifest)$",
            "Metadata tags json": "metadata_tags.json",
            "KMZ": "(\.kmz)$|(\.kml)$",
            "hdf2": "(\.hdf)$"
        }

        # Ingest statistics
        self.files_properties_errors = 0
        self.database_errors = 0
        self.files_indexed = 0
        self.total_files = 0

        # Elasticsearch indexing specific options
        self.index = config['INDEX']
        self.doc_type = 'file'
        self.blocksize = 800

        if config['HOST'] is None:
            self.host = 'jasmin-es1.ceda.ac.uk'
        else:
            self.host = config['HOST']

        if config['PORT'] is None:
            self.port = 9200
        else:
            self.port = config['PORT']

        self.es = Elasticsearch([{'host': self.host, 'port': self.port}])

    def get_level1_file_info(self, file_path):
        """
        Get level 1 file information from NLA and file name
        :param file_path: full path to file
        :return: python dict to be converted into json es document.
        """

        file_info = {}
        info = {}

        info['name'] = os.path.basename(file_path)
        info['name_auto'] = info['name']
        info['directory'] = os.path.dirname(file_path)
        info['location'] = "on_tape"
        info['size'] = round(self.file_dict[file_path] / (1024 * 1024.0), 3)
        info['md5'] = ""

        file_type = os.path.splitext(info['name'])[1]
        if len(file_type) == 0:
            file_type = "File without extension."

        info['type'] = file_type

        file_format = self._get_format(file_path)
        if file_format:
            info['format'] = file_format

        file_info['info'] = info
        return file_info

    def _get_format(self, file_path):

        for file_format, pattern in self.regex_patterns.iteritems():
            if re.search(pattern, file_path):
                return file_format

    def create_bulk_index_json(self, file_list, blocksize):
        """
        Creates the JSON required for the bulk index operation. Also produces an array of files which directly match
        the index JSON. This is to get around any problems caused by files with properties errors which produces None
        when self.process_file_seq is called.

        :param file_list: List of files to create actions from
        :param level: Level of detail to get from file
        :param blocksize: Size of chunks to send to Elasticsearch.

        :return: bulk_list - list of JSON strings to send to ES,
                 files_to_index - list of lists with each inner list containing the matching files to the query.
        """
        bulk_json = ""
        bulk_list = []
        files_to_index = []
        file_array = []

        for i, filename in enumerate(file_list, 1):
            doc = self.get_level1_file_info(filename)

            if doc is not None:
                es_id = hashlib.sha1(filename).hexdigest()

                action = json.dumps({"index": {"_index": self.index, "_type": self.doc_type, "_id": es_id}}) + "\n"
                body = json.dumps(doc) + "\n"

                bulk_json += action + body
                file_array.append(filename)
            else:
                self.files_properties_errors += 1

            if i % blocksize == 0:
                # json_len = bulk_json.count("\n") / 2
                bulk_list.append(bulk_json)
                files_to_index.append(file_array)

                # Reset building blocks
                bulk_json = ""
                file_array = []

        if bulk_json:
            # Add any remaining files
            bulk_list.append(bulk_json)
            files_to_index.append(file_array)

        return bulk_list, files_to_index

    def bulk_index(self, files_to_index):
        """
        Creates the JSON and performs a bulk index operation
        """
        action_list, files_to_index = self.create_bulk_index_json(files_to_index, self.blocksize)

        for action, files in zip(action_list, files_to_index):
            r = self.es.bulk(body=action, request_timeout=60)
            self.process_response_for_errors(r, files)
        print "Files indexed: {} Database Errors: {} Properties errors: {}".format(self.files_indexed,
                                                                                   self.database_errors,
                                                                                   self.files_properties_errors)

    def process_response_for_errors(self, response, files):
        """
        Process and flag up any errors if there were any.
        :param response:
        :param files:
        :return:
        """
        if response['errors']:
            for i, item in enumerate(response['items']):
                if item['index']['status'] not in [200, 201]:
                    # filename = files[i]
                    # error = item['index']['error']
                    # ex = ": ".join([error['type'], error['reason']])
                    # self.logger.error("Indexing error: %s" % ex)
                    # self.logger.error(("%s|%s|%s|%s ms" % (
                    # os.path.basename(filename), os.path.dirname(filename), self.FILE_INDEX_ERROR, ' ')))
                    self.database_errors += 1

                else:
                    self.files_indexed += 1
        else:
            batch_count = len(files)
            self.files_indexed += batch_count
            # self.logger.debug("Added %i files to index" % batch_count)

    def sync_NLA_file_location(self):

        es_update = updater.ElasticsearchUpdater(self.index, self.host, self.port)

        params, query = ElasticsearchQuery.ceda_fbs()

        files_not_in_index = es_update.update_location(self.file_list, params, query, on_disk=self.location)

        return files_not_in_index


def main():
    config = docopt(__doc__, version=pkg_resources.require("ceda_elasticsearch_tools")[0].version)

    sync = NLASync(config)

    # Update all files matched in the index and return list of files that did not match.
    files_not_in_index, summary = sync.sync_NLA_file_location()
    print summary

    # If --index-doc flag set then attempt to clean up the missing files
    if config['--index-docs'] is True:
        if files_not_in_index:
            print ('Pushing missing files to the index')

            if config['--on-disk']:
                # Write an output file which can then be picked up by the FBS scanning process.
                file_name = os.path.basename(config['INPUT_FILE']).split('.')[0] + 'missing.txt'
                output_file = os.path.join(config['OUTPUT_DIR'],'missing_files_on_disk', file_name)
                with open(output_file, 'w') as writer:
                    writer.writelines([x + '\n' for x in files_not_in_index])

            else:
                # If dealing with files where the primary version is on tape.
                # Retrieve level 1 information from NLA and index.
                sync.bulk_index(files_not_in_index)

        # Remove the input file once the task has been completed so that the jobs can be restarted without repeating
        os.remove(config['INPUT_FILE'])


if __name__ == '__main__':
    main()
