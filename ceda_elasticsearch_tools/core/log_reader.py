import requests
import os, sys
from elasticsearch import Elasticsearch
import simplejson as json
import logging
import re
import hashlib
from datetime import datetime

class SpotMapping(object):
    """
    Downloads the spot mapping from the cedaarchiveapp.
    Makes two queryable dicts:
        spot2pathmapping = provide spot and return file path
        path2spotmapping = provide a file path and the spot will be returned
    """
    url = "http://cedaarchiveapp.ceda.ac.uk/cedaarchiveapp/fileset/download_conf/"
    spot2pathmapping = {}
    path2spotmapping = {}

    def __init__(self, test=False):

        if test:
            self.spot2pathmapping['spot-1400-accacia'] = "/badc/accacia"
            self.spot2pathmapping['abacus'] = "/badc/abacus"
        else:
            response = requests.get(self.url)
            log_mapping = response.text.split('\n')

            for line in log_mapping:
                if not line.strip(): continue
                spot, path = line.strip().split()
                if spot in ("spot-2502-backup-test",): continue
                self.spot2pathmapping[spot] = path
                self.path2spotmapping[path] = spot

    def __iter__(self):
        for k in self.spot2pathmapping.keys():
            yield k

    def get_archive_root(self, key):
        """

        :param key: Provide the spot
        :return: Returns the directory mapped to that spot
        """
        return self.spot2pathmapping[key]

    def get_spot(self, key):
        """
        The directory stored in elasticsearch is the basename for the specific file. The directory stored on the spots
        page is further up the directory structure but there is no common cut off point as it depends on how many files there
        are in each dataset. This function recursively starts at the end of the directory stored in elasticsearch
        and gradually moves back up the file structure until it finds a match in the path2spot dict.

        :param key: Provide a filename or directory
        :return: Returns the spot which encompasses that file or directory.
        """

        while (key not in self.path2spotmapping) and (key != '/'):
            key = os.path.dirname(key)

        return self.path2spotmapping[key]


class MD5LogFile(object):
    """
    Reads the log file and creates a dictionary which can be queried using get_md5 and a test string.
    """

    def __init__(self, spot, base_dir):
        """
        Reads the latest log file and stores the checksums in a dict.

        :param spot: Spot name for the directory
        :param base_dir: The base directory to the file as returned by elasticsearch. This is the key used to return the directory filepath.
        """
        self.md5s = {}

        log_dir = "/datacentre/stats/checkm"
        spot_dir = os.path.join(log_dir, spot)

        # Take the spot directory and find the latest log file.
        latest_log_file = get_latest_log(spot_dir, "checkm.")

        # Log filepath = log_dir/latest_log_file
        log_path = os.path.join(spot_dir, latest_log_file)

        with open(log_path) as reader:
            for line in reader:
                if not line.startswith('#'):
                    if line.find("|") > -1:
                        # e.g. line: metadata/csml/seviri_frp.xml|md5|69b829decea5563e33b0856ec80a0c83|806321|2010-04-29T11:10:13Z
                        path, cksum_type, cksum, _1, _2 = line.strip().split("|")
                        line_path = os.path.join(base_dir, path)
                        self.md5s[line_path] = cksum
        if len(self.md5s) == 0:
            print "md5s not found in logfile: %s" % log_path

    def __len__(self):
        return len(self.md5s)

    def as_list(self):
        return list(self.md5s)

    def get_md5(self, path):
        """
        Return the md5 checksum given a filepath.

        :param path: full file path of the object to get md5 for.
        :return: the md5 checksum. If file not found, returns empty string.
        """

        if path in self.md5s:
            return self.md5s[path]
        else:
            logging.WARNING("MD5LogFile: get_md5. Unable to find md5 for %s" % path)
            return ""


class DepositLog(object):
    """
    Object to read an use deposit log file to update elasticsearch and generate md5 checksums.
    """
    log_dir = "/badc/ARCHIVE_INFO/deposit_logs"
    file_list = []
    log_list = []

    def __iter__(self):
        for file in self.file_list:
            yield file

    def __len__(self):
        return len(self.file_list)

    def __getitem__(self, index):
        return self.file_list[index]

    # def __str__(self):
    #     return self.file_list

    def read_log(self, log_filename=None):
        """
        Reads the deposit log into memory and creates a list if newly deposited files as part of the object.

        :param log_filename: Allows the user to specify a log to open. Defaults to the most recent.
        """
        # Make sure file_list is clear before reading file.
        self.file_list = []

        if log_filename is None:
            # If no log file provided, use the latest log file.
            log_filename = get_latest_log(self.log_dir, "deposit_ingest1.")

        with open(os.path.join(self.log_dir, log_filename)) as reader:
            # date regex
            pattern = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:DEPOSIT:")

            for line in reader:
                # Check line matches expression. ie. starts with a date and matches action DEPOSIT.
                if pattern.match(line):
                    # Split line into its components.
                    # e.g line: 2017-08-20 03:05:03:/badc/msg/data/hritimages/EWXT11/2017/08/19/EWXT11_201708190300.png:DEPOSIT:1388172: (force=None) /datacentre/arrivals/users/dartmetoffice/ukmo-msg/EWXT11_201708190300.png
                    date_hour, min, sec, filepath, action, filesize, message = line.strip().split(":")
                    self.file_list.append(filepath)

        return self.file_list

    def generate_md5(self, file):
        """
        Generate md5 checksum for input file.

        :param file: Full path to a real file on the system.
        :return: md5 checksum or "" if file not found.
        """
        if not os.path.exists(file):
            logging.WARNING("DepositLog: generate_md5. File does not exist: %s" % file)
            return ""

        hash_md5 = hashlib.md5()
        with open(file, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def es_deposits_diff(self, index=None):
        """
        Looks at the log files from oldest to newest and checks elasticsearch to see if those files are in.
        Generates a list of files to index.
        :return: None
        """

        # Setup Elasticsearch connection
        esu = ElasticsearchUpdater(index)

        # Get list of logfiles
        logs = sorted([dr for dr in os.listdir(self.log_dir) if dr.startswith("deposit_ingest1.")], reverse=True)

        for log in logs:
            start = datetime.now()
            log_data = self.read_log(log)
            result = esu.check_files_existence(log_data)
            if result["False"]:
                print log, len(result["False"]), "Processing took: %s" % (datetime.now() - start)



class ElasticsearchUpdater(object):
    """
    Class to handle updates to the elasticsearch index.
    """

    def __init__(self, index, host="jasmin-es1.ceda.ac.uk", port=9200):
        """
        Creates an elasticsearch connection. Default host and port specified.

        :param index: The elasticsearch index to connect to.
        :param host: The elasticsearch host address.
        :param port: The read/write elasticsearch port.
        """

        self.es = Elasticsearch(hosts=[{"host": host, "port": port}])
        self.index = index

    def get_md5_bulk_update_from_spotlog(self, spot_name, spotmapobj, log_base_dir, update=False, threshold=800):
        """

        :param spot_name: The name of the spot to check for updates.
        :param spotobj: Spotmapping object
        :param log_base_dir: The base dir path for the output logs.
        :param update: Boolean value to state whether to update ES live or to return the JSON needed by the bulk updater.
        :param threshold: Maximum number of items to send to the bulk API.

        :return: JSON for use by the bulk updater API. If update is true, the updates will have completed and so the
        md5_json will be "".
        """

        spotlog = MD5LogFile(spot_name, spotmapobj.get_archive_root(spot_name))
        file_list = spotlog.as_list()


        # Set intial values
        update_total = 0
        md5_json = ""
        spot_log_path = os.path.join(log_base_dir, 'not_found',datetime.now().strftime("%Y-%m-%d"), spot_name)

        if not os.path.exists(spot_log_path):
            os.makedirs(spot_log_path)

        with open(os.path.join(spot_log_path, 'files_not_found.txt'), 'a') as logger:
            logger.write(
                "# List of files which are listed in the spot logs but not found in Elasticsearch index: %s.\n\n" % self.index)

            result = self.check_files_existence(file_list,threshold=threshold, raw_resp=True)

            files_in_es = result["True"]
            files_not_in_es = result["False"]

            # process files not in es and write to log file.
            for file in files_not_in_es:
                logger.write("File not found: %s \n" % file)


            # Check md5
            for file in files_in_es:
                file_info = file[0]["_source"]["info"]
                filepath = os.path.join(file_info["directory"], file_info["name"])

                if file_info["md5"] != spotlog.get_md5(filepath):
                    update_total += 1

                    id = file[0]["_id"]
                    index = json.dumps({"update": {"_id": id, "_type": "file"}}) + "\n"
                    md5_field = json.dumps({"source": {"doc": {"info": {"md5": spotlog.get_md5(filepath)}}}}) + "\n"
                    md5_json += index + md5_field

                if update and update_total > threshold:
                    self.make_bulk_update(md5_json)
                    md5_json = ""
                    update_total = 0

            if update:
                self.make_bulk_update(md5_json)
                md5_json = ""

        return md5_json




    def make_bulk_update(self, bulk_json):
        """
        Use the ES bulk API to make a bulk update to the ES index specified by the object.

        :param bulk_json: JSON to execute in the bulk request.
        :return: Status of the transaction.
        """
        if bulk_json:
            result = self.es.bulk(index=self.index, body=bulk_json)
            return {"took": result["took"], "errors": result["errors"], "docs_changed": len(result["items"])}
        else:
            return {"took": 0, "errors": "True", "error_msg": "No JSON submitted for updates", "docs_changed": 0}


    def check_files_existence(self, file_list=[], raw_resp=False, threshold=800):
        """
        Given a list of files in the archive, return a dictionary containing files from archive which are present in
        the given index; dict["True"] and those which are not; dict["False"].

        :param file_list: List of real file paths.
        :param raw_resp: Boolean to state whether to include the ES response in the return dict or not.
        :param threshold: Limit for Elasticsearch msearch API call.

        :return: A dict comprising two lists. Files from the supplied list present in given ES index and those not.
                dict{"True"[List of the files provided which are indexed],"False":[List of files provided not indexed]}
        """

        # Set defaults
        file_count = 0
        scroll_count = 0
        multi_search_query = ""
        file_in_index = {"True": [], "False": []}

        # Return if no file list has been provided
        if not file_list:
            return file_in_index

        query_templ = json.dumps({"query": {"bool": {"must": {"match": {"info.directory": "<dirname>"}},
                                                          "filter": {
                                                              "term": {"info.name": "<filename>"}}}}})

        # function to generate the parameters to render in the query_templ
        def params(item):
            return {"filename": os.path.basename(item), "dirname": os.path.dirname(item)}

        msearch_query_list = self.gen_msearch_json(query_templ, params, file_list, blocksize=threshold)

        file_in_index = self._get_and_process_results(msearch_query_list,file_list, threshold, file_in_index, raw_resp)


        return file_in_index

    def gen_msearch_json(self, querytemp, paramfunc, input_list, blocksize):
        """
        Takes a list and creates an Elasticsearch msearch query with the desired blocksize.
        The query is passed in using querytemp and the paramfunc defines the parameters which
        will be rendered to produce the final query.

        :param querytemp: Template query JSON
        :param paramfunc: Function which returns the parameters needed in the querytemp. eg.

                          def params(item)
                                return {"dirname": os.path.dirname(item), "filename":os.path.filename(item)}

                          This should be passed in without brackets.
        :param input_list: List to turn into a query.
        :param blocksize: Number of files to include in each msearch query.

        :return: List with each element containing a JSON msearch query which has been chopped so that the number of
                 objects in the query matches blocksize.
        """
        msearch_json = ""
        query_list = []

        for i, item in enumerate(input_list, 1):
            params = paramfunc(item)

            index = json.dumps({}) + "\n"
            search_query = self._render_query(querytemp, params) + "\n"

            msearch_json += index + search_query

            if i % blocksize == 0:
                query_list.append(msearch_json)
                msearch_json = ""

        if msearch_json:
            query_list.append(msearch_json)

        return query_list


    def _render_query(self, query, parameters):
        """
        Renders parameters into JSON for elasticsearch query.
        Templated variables are in the format <var1>

        :param query: The query template with dynamic vars with format <var1>
        :param parameters: Dictionary containing key, value pairs for the template eg. {"var1":"Test string"}
                           would replace <var1> in the query template.

        :return: Returns a JSON string with variables templated in.
        """
        m = re.findall('<\w+>', query)
        for match in m:
            param = parameters[match.strip('<>')]
            query = query.replace(match, param)

        return query

    def _get_and_process_results(self, msearchquery_list, file_list, blocksize, output, raw_resp):
        """
        Generate a True False dict of filepaths contained in the index from a suppled file list.

        :param msearchquery_list: A list containing msearch query JSON split into blocks.
        :param file_list: List of filepaths to test.
        :param blocksize: Max number of files included in each query.
        :param output: the output dictionary

        :return: True False dict of file paths in given index: self.index
        """

        scroll_count = 0
        for mquery in msearchquery_list:

            results = self.es.msearch(index=self.index, body=mquery)

            if raw_resp:
                for i, response in enumerate(results["responses"]):
                    if raw_resp:
                        # Append the raw ElasticSearch response where there is data.
                        if response["hits"]["total"] == 1:
                            output["True"].append(response["hits"]["hits"])

                        if response["hits"]["total"] == 0:
                            output["False"].append(file_list[i + (blocksize * scroll_count)])

                    else:
                        # Append the filepath
                        if response["hits"]["total"] == 1:
                            output["True"].append(file_list[i + (blocksize * scroll_count)])

                        if response["hits"]["total"] == 0:
                            output["False"].append(file_list[i + (blocksize * scroll_count)])



            scroll_count+= 1

        return output


def get_latest_log(dir, prefix):
    """
    Get the latest log file.

    :param dir: The directory to test
    :param prefix: The log specific file prefix.

    :return: The most recent log file.
    """
    return sorted([dr for dr in os.listdir(dir) if dr.startswith(prefix)])[-1]




# if __name__ == '__main__':

    # index = 'ceda-archive-level-1'
    # log_dir = 'log'
    #
    # host = "jasmin-es1.ceda.ac.uk"
    #
    # port = 9200
    #
    # logging_path = log_dir
    # if not os.path.exists(logging_path):
    #     os.makedirs(logging_path)
    #
    # # initiate log
    # log_name = "md5_update-" + datetime.now().strftime("%Y-%m-%d") + ".log"
    # logging.basicConfig(filename=os.path.join(logging_path, log_name), level=logging.WARNING)
    #
    # spots = SpotMapping()
    # update = ElasticsearchUpdater(index=index, host=host, port=port)
    #
    # begin = datetime.now()
    # for spot in spots:
    #     start = datetime.now()
    #     update.get_md5_bulk_update_from_spotlog(spot_name=spot, spotmapobj=spots, log_base_dir=logging_path, update=True)
    #     print "Spot: %s took: %s to analyse." % (spot, (datetime.now()-start))
    #
    # print "Whole operation took: %s" % (datetime.now()-begin)

