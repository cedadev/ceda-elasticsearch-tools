import requests
import os
import re
import hashlib
from datetime import datetime
from ceda_elasticsearch_tools.core.utils import get_latest_log

import logging


class SpotMapping(object):
    """
    Downloads the spot mapping from the cedaarchiveapp.
    Makes two queryable dicts:
        - spot2pathmapping = provide spot and return file path
        - path2spotmapping = provide a file path and the spot will be returned
    """
    url = "http://cedaarchiveapp.ceda.ac.uk/cedaarchiveapp/fileset/download_conf/"
    spot2pathmapping = {}
    path2spotmapping = {}

    # Remove logging message when running script
    logging.getLogger("requests").setLevel(logging.WARNING)

    def __init__(self, test=False, spot_file=None, sep='='):

        if test:
            self.spot2pathmapping['spot-1400-accacia'] = "/badc/accacia"
            self.spot2pathmapping['abacus'] = "/badc/abacus"

        elif spot_file:
            with open(spot_file) as reader:
                spot_mapping = reader.readlines()

            self._build_mapping(spot_mapping, sep=sep)

        else:
            self._download_mapping()

    def __iter__(self):
        return iter(self.spot2pathmapping)

    def __len__(self):
        return len(self.spot2pathmapping)

    def _download_mapping(self):
        """
        Download the mapping from the cedaarchiveapp and build mappings.
        """

        response = requests.get(self.url)
        spot_mapping = response.text.split('\n')

        self._build_mapping(spot_mapping)

    def _build_mapping(self, spot_mapping, sep=None):
        """
        Build the spot mapping dictionaries
        :param spot_mapping: list of mappings
        """

        for line in spot_mapping:
            if not line.strip(): continue
            spot, path = line.strip().split(sep)

            if spot in ("spot-2502-backup-test",): continue
            self.spot2pathmapping[spot] = path
            self.path2spotmapping[path] = spot


    def get_archive_root(self, key):
        """

        :param key: Provide the spot
        :return: Returns the directory mapped to that spot
        """

        # Get the path to the spot
        archive_root = self.spot2pathmapping.get(key)

        # If key not found, refresh the mapping and try again
        # If still not found, this will return None
        if archive_root is None:
            self._download_mapping()

            archive_root = self.spot2pathmapping.get(key)

        return archive_root

    def get_spot(self, key):
        """
        The directory stored in elasticsearch is the basename for the specific file. The directory stored on the spots
        page is further up the directory structure but there is no common cut off point as it depends on how many files there
        are in each dataset. This function recursively starts at the end of the directory stored in elasticsearch
        and gradually moves back up the file structure until it finds a match in the path2spot dict.

        :param key: Provide a filename or directory
        :return: Returns the spot which encompasses that file or directory.
        """

        archive_path = self.get_archive_path(key)

        while (archive_path not in self.path2spotmapping) and (archive_path != '/'):
            archive_path = os.path.dirname(archive_path)

        if archive_path == '/':
            return None

        return self.path2spotmapping[archive_path]

    def get_spot_from_storage_path(self, path):
        """
        Extract the spot name from the storage path

        :param path: Path to test
        :return: spot name and path suffix
        """
        storage_suffix = path.split('archive/')[1]
        spot = storage_suffix.split('/')[0]
        try:
            suffix = storage_suffix.split(spot + '/')[1]

        except IndexError:
            suffix = None

        return spot, suffix

    def get_archive_path(self, path):
        storage_path = os.path.realpath(path)

        spot, suffix = self.get_spot_from_storage_path(storage_path)

        spot_path = self.get_archive_root(spot)

        try:
            archive_path = os.path.join(spot_path, suffix)

        # Joining None produces an AttributeError in py2 and a TypeError py3
        except (AttributeError, TypeError):
            archive_path = spot_path

        return archive_path

    def is_archive_path(self, path):
        """
        Archive path refers to the location in the archive where the file exists.
        In the archive there are 3 path types:
            - storage path: the actual location of the file eg. /datacentre/archvol3/pan125/archive/namblex/data/...
            - archive path: the path with the spot mapping replacing the storage prefix eg. /badc/namblex/data/aber-radar-1290mhz/20020831/
            - symlink path: an alternative route to the file as displayed using pydap eg. badc/ncas-observations/data/man-radar-1290mhz/previous-versions/2002/08/31/

        This function takes the input path, gets the storage path, replaces the storage prefix with spot mapping to get the archive
        path and compares it to the input path. Returns True if input path is archive path.

        :param path: file path to test
        :return: Bool
        """

        return path == self.get_archive_path(path)


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

        if not os.path.exists(spot_dir):
            return

        # Take the spot directory and find the latest log file.
        latest_log_file = get_latest_log(spot_dir, "checkm.")

        if latest_log_file:

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
                pass
                # print("md5s not found in logfile: %s" % log_path)

    def __len__(self):
        return len(self.md5s)

    def __iter__(self):
        return iter(self.md5s)

    def __getitem__(self, key):
        return self.md5s[key]

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
            return ""


class DepositLog(object):
    """
    Object to read an use deposit log file to update elasticsearch and generate md5 checksums.
    """
    log_dir = "/badc/ARCHIVE_INFO/deposit_logs"
    deposit_list = []
    deletion_list = []
    filename = None

    def __init__(self, log_filename=None):
        """
        Reads the deposit log into memory and creates a list if newly deposited files as part of the object.

        :param log_filename: Allows the user to specify a log to open. Defaults to the most recent.
        """
        # Make sure deposit_list/deletion_list is clear before reading file.
        self.deposit_list = []
        self.deletion_list = []
        self.mkdir_list = []
        self.rmdir_list = []
        self.symlink_list = []
        self.readme00_list = []

        if log_filename is None:
            # If no log file provided, use the penultimate log file. eg. Most recent complete log file.
            log_filename = sorted([dr for dr in os.listdir(self.log_dir) if dr.startswith('deposit_ingest1.')])[-2]

        self.filename = log_filename

        with open(os.path.join(self.log_dir, log_filename)) as reader:
            # date regex
            deposit = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:DEPOSIT:")

            deletion = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:REMOVE:")

            mkdir = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:MKDIR:")

            rmdir = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:RMDIR:")

            symlink = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:SYMLINK:")

            readme00 = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*00README:")

            for line in reader:
                # Split line into its components.
                # e.g line: 2017-08-20 03:05:03:/badc/msg/data/hritimages/EWXT11/2017/08/19/EWXT11_201708190300.png:DEPOSIT:1388172: (force=None) /datacentre/arrivals/users/dartmetoffice/ukmo-msg/EWXT11_201708190300.png

                split_line = line.strip().split(":")

                date_hour = split_line[0]
                min = split_line[1]
                sec = split_line[2]
                filepath = split_line[3]
                action = split_line[4]
                filesize = split_line[5]
                message = ":".join(split_line[6:])

                if deposit.match(line):
                    self.deposit_list.append(filepath)

                    # Add to readme list if the deposited file is a 00README
                    if readme00.match(line):
                        self.readme00_list.append(filepath)

                elif deletion.match(line):
                    self.deletion_list.append(filepath)

                elif mkdir.match(line):
                    self.mkdir_list.append(filepath)

                elif rmdir.match(line):
                    self.rmdir_list.append(filepath)

                elif symlink.match(line):
                    self.symlink_list.append(filepath)

    def __iter__(self):
        for file in self.deposit_list:
            yield file

    def __len__(self):
        return len(self.deposit_list)

    def __getitem__(self, index, action="DEPOSIT"):

        if action == "DEPOSIT":
            return self.deposit_list[index]

        elif action == "REMOVE":
            return self.deletion_list[index]

        elif action == "MKDIR":
            return self.mkdir_list[index]

        elif action == "RMDIR":
            return self.rmdir_list[index]

        elif action == "SYMLINK":
            return self.symlink_list[index]


    def read_log(self):
        return self.deposit_list

    def write_filelist(self, destination):
        output = [x + "\n" for x in self.deposit_list]
        with open(destination, 'w') as writer:
            writer.writelines(output)

    def write_deletionlist(self, destination):
        output = [x + "\n" for x in self.deletion_list]
        with open(destination, 'w') as writer:
            writer.writelines(output)

    def generate_md5(self, file):
        """
        Generate md5 checksum for input file.

        :param file: Full path to a real file on the system.
        :return: md5 checksum or "" if file not found.
        """
        if not os.path.exists(file):
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
        from updater import ElasticsearchUpdater

        # Setup Elasticsearch connection
        esu = ElasticsearchUpdater(index)

        # Get list of logfiles
        logs = sorted([dr for dr in os.listdir(self.log_dir) if dr.startswith("deposit_ingest1.")], reverse=True)

        for log in logs:
            start = datetime.now()
            log_data = self.read_log(log)
            result = esu.check_files_existence(log_data)
            if result["False"]:
                print(log, len(result["False"]), "Processing took: %s" % (datetime.now() - start))
