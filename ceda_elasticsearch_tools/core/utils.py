import subprocess
from datetime import datetime, timedelta
import sys
import os
import math

def get_number_of_submitted_lotus_tasks():

    """
    :returns: Number of tasks submitted in lotus.
    """

    empty_task_queue_string = "No unfinished job found\n"
    non_empty_task_queue_string = "JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME"

    command_output = subprocess.check_output('bjobs', stderr=subprocess.STDOUT, shell=True)

    if command_output == empty_task_queue_string:
        num_of_running_tasks = 0
    else:
        num_of_running_tasks = command_output.count("\n") -1

    return num_of_running_tasks

def _make_bsub_command(task):
    "Construct bsub command for task and return it."
    command = "bsub -q par-single -W 48:00 %s" % task
    return command

def percent(total, value):
    if total == 0:
        return 100
    elif value == 0:
        return 0
    else:
        return round((float(value)/ total) * 100,2)

def get_latest_log(dir, prefix, rank=-1):
    """
    Get the log file from each stream with the given prefix and rank.
    Logs are sorted in date oldest to newest. Positive rank will start at
    at the old logs. 1 will return the oldest log, 2 the next oldest. Negative rank will be from newest backwards. -1 will return
    the most recent log, -2 the penultimate log.

    :param dir:     The directory to test
    :param prefix:  The log specific file prefix.
    :param rank:    Which item to select. Defaults to the most recent.

    :return: List of logs for all streams with the given prefix and rank.
    """

    # Get all logs in the directory
    dir_listing = os.listdir(dir)

    # Determine all ingest streams with the given prefix
    ingest_streams = set([log.split('.')[0] for log in dir_listing if log.startswith(prefix)])

    # Create output list
    latest_logs = []
    for stream in ingest_streams:

        # Filter logs on the given stream
        filtered_logs = [log for log in dir_listing if log.startswith(stream)]

        # Check to make sure the rank is in acceptable range
        if abs(rank) > len(filtered_logs):
            # When rank is outside the range of the log list, return the last possible value.
            rank = math.copysign(len(filtered_logs), rank)

        # Find the log with correct rank
        latest = sorted([log for log in filtered_logs if log.startswith(stream)])[rank]
        latest_logs.append(latest)

    return latest_logs


class ProgressBar(object):

    def __init__(self, endvalue, label='Percent', bar_length=50):
        self.endvalue = endvalue
        self.label = label
        self.bar_length = bar_length
        self.start = datetime.now()

    def est_time(self, progress):
        """
        Return tuple with elapsed time and predicted time to completion.

        :param progress: Percent complete
        :return: Elaspsed, Predicted time remaining
        """

        if progress < 0.01:
            # At start of
            return("00:00:00","--:--:--")

        else:
            end = datetime.today()
            dif = end - self.start
            elapsed = str(dif).split('.')[0]
            est_runtime = timedelta(seconds=(1/progress) * dif.total_seconds())
            remain = est_runtime - dif
            est = str(remain).split('.')[0]

        return(elapsed,est)

    def running(self, value):
        """
        :param value: The current progress
        """

        percent = float(value) / self.endvalue
        elapsed, time_remain = self.est_time(percent)

        progress = '#' * int(round(percent * self.bar_length))
        spaces = ' ' * (self.bar_length - len(progress))

        sys.stdout.write("\r{0}: [{1}] {2}% ({3}/{4})".format(self.label, progress + spaces, round(percent * 100,1), elapsed, time_remain))
        sys.stdout.flush()

    def complete(self):
        """
        Prints a completed progress bar to screen
        :return:
        """
        end = str(datetime.now() - self.start).split('.')[0]
        progress = '#' * self.bar_length
        sys.stdout.write("\r{0}: [{1}] {2}% ({3}/00:00:00)".format(self.label, progress, 100, end))
        sys.stdout.flush()
        sys.stdout.write("\n")