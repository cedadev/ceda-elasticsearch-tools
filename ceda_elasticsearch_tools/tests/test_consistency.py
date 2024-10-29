
class TestConsistency:
    
    def test_index_tools(self):
        from ceda_elasticsearch_tools import IndexUpdaterBase, BulkClient, __version__
        from ceda_elasticsearch_tools.index_tools import (
            CedaDirs,
            CedaEo,
            CedaFbi
        )

        assert 1==1, "Importing index tools successful."

    def test_elasticsearch(self):
        from ceda_elasticsearch_tools import CEDAElasticsearchClient
        from ceda_elasticsearch_tools.elasticsearch.ceda_elasticsearch_client import CA_ROOT

        assert 1==1, "Importing elasticsearch successful."

    def test_core(self):
        from ceda_elasticsearch_tools.core.log_reader import (
            SpotMapping,
            MD5LogFile,
            DepositLog,
        )

        from ceda_elasticsearch_tools.core.updater import (
            ElasticsearchQuery,
            IndexFilter,
            ElasticsearchUpdater
        )

        from ceda_elasticsearch_tools.core.utils import (
            get_number_of_submitted_lotus_tasks,
            percent,
            get_latest_log,
            list2file_newlines,
            ProgressBar
        )

        assert 1==1, "Importing core successful."

    def test_cmdline(self):

        from ceda_elasticsearch_tools.cmdline.ceda_eo.coverage_test import main
        from ceda_elasticsearch_tools.cmdline.secondary_scripts.md5 import (
            logger_setup,
            file_md5,
            main
        )
        from ceda_elasticsearch_tools.cmdline.secondary_scripts.nla_sync_lotus_task import (
            main,
            NLASync
        )

        from ceda_elasticsearch_tools.cmdline.secondary_scripts.spot_checker import (
            es_connection,
            main,
            make_query,
            process_list,
            get_args,
            dir_exists
        )

        from ceda_elasticsearch_tools.cmdline.fbs_missing_files import (
            submit_jobs_to_lotus,
            generate_summary,
            create_missing_list,
            nolotus,
            main,
        )

        from ceda_elasticsearch_tools.cmdline.nla_sync_es import (
            chunk_dict,
            chunks,
            create_output_dir,
            download_data_from_nla,
            main,
            loading
        )

        from ceda_elasticsearch_tools.cmdline.update_md5 import (
            logger_setup,
            update_from_logs,
            extract_id,
            write_page_to_file,
            download_files_missing_md5,
            calculate_md5s,
            main
        )

        assert 1==1, "Importing cmdline successful."
