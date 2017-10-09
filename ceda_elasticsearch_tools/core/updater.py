from elasticsearch import Elasticsearch
import os
import simplejson as json
import re


class ElasticsearchQuery(object):
    """
    Class to hold information for common index access
    """

    @staticmethod
    def ceda_eo_manifest():
        """
        Method returns parameters and query needed by Elasticsearch.check_files_existence() in order to work with
        ceda-eo using the manifest file to search.
        :return:
        """
        query = json.dumps({
            "_source": {
                "include": [
                    "file"
                ]
            },
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "file.path.raw": "<filename>"
                        }
                    }
                }
            }
        })

        def param_func(item):
            return {"filename": os.path.basename(item), "dirname": os.path.dirname(item)}

        return param_func, query

    @staticmethod
    def ceda_eo_data_file():
        """
        Method returns parameters and query needed by Elasticsearch.check_files_existence() in order to work with
        ceda-eo using the datafile to search.
        :return:
        """

        query = json.dumps({"query": {"bool": {"must": {"match": {"file.directory": "<dirname>"}},
                                                          "filter": {
                                                              "term": {"file.data_file.keyword": "<filename>"}}}}})

        def param_func(item):
            return {"filename": os.path.basename(item), "dirname": os.path.dirname(item)}

        return param_func, query

    @staticmethod
    def ceda_fbs():
        """
        Method returns parameters and query needed by Elasticsearch.check_files_existence() in order to work with
        ceda-fbs.
        :return:
            param_func: The function which will be used by the template renderer
            query
        """
        query = json.dumps({"query": {"bool": {"must": {"match": {"info.directory": "<dirname>"}},
                                                          "filter": {
                                                              "term": {"info.name": "<filename>"}}}}})

        def param_func(item):
            return {"filename": os.path.basename(item), "dirname": os.path.dirname(item)}

        return param_func, query


class ElasticsearchUpdater(object):
    """
    Class to handle updates to the elasticsearch index.
    """

    def __init__(self, index, host, port):
        """
        Creates an elasticsearch connection. Default host and port specified.

        :param index: The elasticsearch index to connect to.
        :param host: The elasticsearch host address.
        :param port: The read/write elasticsearch port.
        """

        self.es = Elasticsearch(hosts=[{"host": host, "port": port}])
        self.index = index

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

    def check_files_existence(self, param_func, query_tmpl, file_list=[], raw_resp=False, threshold=800):
        """
        Given a list of files in the archive, return a dictionary containing files from archive which are present in
        the given index; dict["True"] and those which are not; dict["False"].

        :param param_func: function which returns the parameters needed when rendering the query
        :param query_tmpl: The template to contruct the elasticsearch query
        :param file_list: List of real file paths.
        :param raw_resp: Boolean to state whether to include the ES response in the return dict or not.
        :param threshold: Limit for Elasticsearch msearch API call.

        :return: A dict comprising two lists. Files from the supplied list present in given ES index and those not.
                dict{"True"[List of the files provided which are indexed],"False":[List of files provided not indexed]}
        """

        # Set defaults
        file_in_index = {"True": [], "False": []}

        # Return if no file list has been provided
        if not file_list:
            return file_in_index

        msearch_query_list = self.gen_msearch_json(query_tmpl, param_func, file_list, blocksize=threshold)

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

    def gen_bulk_update_json(self, querytemp, paramfunc, input_list, blocksize):
        """
        Takes a list and creates an Elasticsearch bulk update query with the desired blocksize.
        The query is passed in using querytemp and the paramfunc defines the parameters which
        will be rendered to produce the final query.

        :param querytemp: Template query JSON
        :param paramfunc: Function which returns the parameters needed in the querytemp. eg.

                          def params(item)
                                return {"dirname": os.path.dirname(item), "filename":os.path.filename(item)}

                          This should be passed in without brackets.
        :param input_list: List to turn into a query.
        :param blocksize: Number of files to include in each msearch query.

        :return: List with each element containing a JSON bulk query which has been chopped so that the number of
                 objects in the query matches blocksize.
        """
        bulk_json = ""
        query_list = []

        for i, item in enumerate(input_list, 1):
            params = paramfunc(item)

            index = json.dumps({"update": {"type"}}) + "\n"
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

    def update_location(self, file_list, params, search_query, on_disk, threshold=800):
        """
        Currently only works with the ceda-eo index.

        :param file_list: List of file paths to update
        :param params: function which returns parameters
        :param search_query: query used to search index to check existence
        :param on_disk: Boolean. Sets location value to on_disk or on_tape

        :return: 1. Summary string. 2. Result from the bulk API.
        """

        # set location
        if on_disk:
            location = "on_disk"
        else:
            location = "on_tape"

        # Check if files are in the provided index
        index_test = self.check_files_existence(param_func=params,
                                                query_tmpl=search_query,
                                                file_list=file_list,
                                                raw_resp=True)

        # Only update those files which are contained in the target index.
        files_to_update = index_test["True"]

        # create update json and update location
        update_json = ""
        result = []

        for i, file in enumerate(files_to_update,1):
            id = file[0]["_id"]
            index = json.dumps({"update": {"_id": id, "_type": "geo_metadata"}}) + "\n"
            location_field = json.dumps({"source": {"doc": {"file": {"location": location }}}}) + "\n"
            update_json += index + location_field

            if i % threshold == 0:
                result.append(self.make_bulk_update(update_json))
                update_json = ""

        # Clean up any remaining updates
        result.append(self.make_bulk_update(update_json))

        summary_string = "Processed {} files. " \
                         "Updated '{}' index. " \
                         "Updated {} files. " \
                         "{} files not in target index".format(len(file_list),
                                                               self.index,
                                                               i,
                                                               len(index_test["False"])
                                                               )

        return summary_string, result
