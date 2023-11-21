"""
A Snakemake log_handler() function to be loaded with --log-handler-script.

Documentation on the *record* argument from :py:func:``snakemake.snakemake``'s
docstring.  It's not complete, but it's a good start.

    :level:
        the log level ("info", "warning", "debug", "error", "progress", "resources_info", "run_info", "group_info", "job_info", "job_error", "group_error", "dag_debug", "shellcmd", "job_finished", "rule_info", "d3dag")

https://github.com/snakemake/snakemake/blob/d2793223f914790c07b25363cb9b314ef166cb3e/snakemake/logging.py#L335-L337

    :level="info", "error" or "debug":
        :msg:
            the log message

    :level="progress":
        :done:
            number of already executed jobs

        :total:
            number of total jobs

    :level="job_info":
        :input:
            list of input files of a job

        :output:
            list of output files of a job

        :log:
            path to log file of a job

        :local:
            whether a job is executed locally (i.e. ignoring cluster)

        :msg:
            the job message

        :reason:
            the job reason

        :priority:
            the job priority

        :threads:
            the threads of the job
"""

import json
import snakemake
from os import environ
from pathlib import Path
from threading import Lock
from datetime import datetime



log_path = Path(".snakemake") / "log"
log_path.mkdir(parents=True, exist_ok=True)
log_file = datetime.now().isoformat(timespec="minutes").replace(":", "") + ".snakemake.json"

LOG_FILE = open(log_path / log_file, "w", encoding = "utf-8")
LOCK = Lock()

def log_handler(record):
    with LOCK:
        json.dump(
            record,
            LOG_FILE,
            allow_nan = False,
            separators = ",:",
            default = serialize,
            sort_keys = True)
        LOG_FILE.write("\n")
        LOG_FILE.flush()

def serialize(obj):
    if isinstance(obj, snakemake.jobs.Job):
        return {
            "name": obj.name,
            "wildcards": obj.wildcards_dict,
        }
    else:
        return {
            "type": str(type(obj)),
            "repr": repr(obj),
            "str": str(obj),
        }
