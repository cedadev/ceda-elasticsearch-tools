import subprocess
from datetime import datetime, timedelta
import sys

def get_number_of_submitted_lotus_tasks():

    """
    :returns: Number of tasks submitted in lotus.
    """

    empty_task_queue_string = "No unfinished job found\n"
    non_empty_task_queue_string = "JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME"

    command_output = subprocess.check_output('bjobs', stderr=subprocess.STDOUT, shell=True)

    if command_output == empty_task_queue_string:
        num_of_running_tasks = 0
    elif command_output.startswith(non_empty_task_queue_string):
        num_of_running_tasks = command_output.count("\n") -1
    else:
        num_of_running_tasks = max_number_of_tasks_to_submit

    return num_of_running_tasks

def sanitise_args(config):
    """
    Sanitise command-line configuration.

    :param config: Config dictionary (from docopt)
    :returns: Config dictionary with all keys stripped of '<' '>' and '--'
    """
    sane_conf = {}
    for key, value in config.iteritems():
        if value is not None:
            key = key.lstrip("-><").rstrip("><")
            sane_conf[key] = value


    return sane_conf


class ProgressBar(object):

    def __init__(self, endvalue, label='Percent', bar_length=50):
        self.endvalue = endvalue
        self.label = label
        self.bar_length = bar_length
        self.start = None

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
        if not self.start:
            self.start = datetime.now()

        percent = float(value) / self.endvalue
        elapsed, time_remain = self.est_time(percent)

        progress = '#' * int(round(percent * self.bar_length))
        spaces = ' ' * (self.bar_length - len(progress))

        sys.stdout.write("\r{0}: [{1}] {2}% ({3}/{4})".format(self.label, progress + spaces, int(round(percent * 100)), elapsed, time_remain))
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