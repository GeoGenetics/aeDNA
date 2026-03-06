#!/usr/bin/env python

import argparse
import logging
import numpy as np
import pandas as pd
from pathlib import Path


# Log messages and status; order matters!
status_msgs = {
    "aeDNA workflow finished successfully!": "OK",
    "steps (100%) done": "OK",
    "Directory cannot be locked": "LOCKED",
    "aeDNA workflow finished with an error!": "ERROR",
    "At least one job did not complete successfully.": "ERROR",
    "HTTPError:": "ERROR_NETWORK",
    ": error: ": "ERROR_SLURM",
    "Cleaning up log files older than ": "NOT_RUN",
    "NOT_EXIST": "NOT_EXIST",
    "RUNNING": "RUNNING",
    "PENDING": "PENDING",
    "ERROR_OOM": "ERROR_OOM",
    "NODE_FAIL": "NODE_FAIL",
    "UNKNOWN": "UNKNOWN",
}


def parse_snakemake_logs(log, n_lines=10):
    if isinstance(log, Path):
        with open(log, "r") as log_fh:
            # Read log file
            log = log_fh.read().splitlines()
            # Get last unique n_lines
            log_tail = list(dict.fromkeys(reversed(log)))[0:n_lines]
            # Search for error messages
            for msg, status in status_msgs.items():
                for log_msg in log_tail:
                    if msg in log_msg:
                        return status
            return pd.NA
    elif pd.isna(log):
        return "NOT_RUN"
    else:
        return pd.NA


# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Check status of jobs.",
    allow_abbrev=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-i",
    "--job-list",
    action="store",
    type=Path,
    nargs="+",
    default=["/dev/stdin"],
    help="Path to stats file",
)
parser.add_argument(
    "--scheduler",
    action="store",
    default="slurm",
    help="HPC scheduler",
)
parser.add_argument(
    "--hpc-extra",
    action="store",
    default="--starttime now-10days",
    help="HPC extra query arguments",
)
parser.add_argument(
    "-s",
    "--status",
    action="store",
    nargs="+",
    choices=set(status_msgs.values()),
    help="Status of jobs to print",
)
parser.add_argument(
    "-l",
    "--loglevel",
    action="store",
    default="INFO",
    choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    help="Log verbosity level",
)
args = parser.parse_args()


# Set logger
loglevel = getattr(logging, args.loglevel.upper(), None)
logging.basicConfig(
    encoding="utf-8",
    level=loglevel,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# Read job list
logging.info("Reading input file(s)")
df = pd.concat(
    [
        pd.read_table(
            job_list, header=None, index_col=0, usecols=[0], comment="#"
        ).assign(filename=job_list)
        for job_list in args.job_list
    ]
)
n_jobs = df.shape[0]
logging.info(f"Checking {n_jobs} jobs")


######################
### Snakemake logs ###
######################
logging.info("Looking up Snakemake log")
df["snakemake_log"] = df.index.map(
    lambda x: (
        [pd.NA] + sorted((Path(x) / ".snakemake" / "log").glob("20*.*.snakemake.log"))
    ).pop()
)
# Parse Snakemake logs
df["status"] = df["snakemake_log"].map(lambda x: parse_snakemake_logs(x))
# Check locked runs
locked_runs = df.index.map(
    lambda x: any((Path(x) / ".snakemake" / "locks").glob("*.lock"))
)
df.loc[locked_runs, "status"] = "LOCKED"
# Check non-existing runs
exist_runs = df.index.map(lambda x: Path(x).exists())
df.loc[~exist_runs, "status"] = "NOT_EXIST"
logging.debug(f"\n{df}")


################
### HPC logs ###
################
logging.info("Querying HPC account manager")
if args.scheduler == "":
    logging.info("No HPC scheduler provided. Using Snakemake logs only!")
    df["status"] = df["status"].fillna("UNKNOWN")
elif args.scheduler == "slurm":
    # Add HPC scheduler stats
    import subprocess
    from io import StringIO

    hpc_cmd = [
        "sacct",
        "--allocations",
        "--parsable2",
        "--endtime",
        "now",
        "--format",
        "JobId,JobName%-500,State,End",
    ] + args.hpc_extra.split(" ")
    logging.debug(" ".join(hpc_cmd))

    res = subprocess.run(
        hpc_cmd,
        stdout=subprocess.PIPE,
    )
    res = (
        pd.read_csv(StringIO(res.stdout.decode("utf-8")), sep="|", low_memory=False)
        .rename(
            columns={
                "JobID": "id",
                "JobName": "name",
                "State": "hpc_status",
                "End": "time_end",
            }
        )
        .astype({"id": np.uint64})
        .sort_values("time_end")
        .set_index(["name", "id"])
    )

    # Rename status
    res["hpc_status"] = res["hpc_status"].replace(
        {"COMPLETED": "OK", "FAILED": "ERROR", "OUT_OF_MEMORY": "ERROR_OOM"}
    )
    res.loc[res["hpc_status"].str.startswith("CANCELLED"), "hpc_status"] = "ERROR"
    # Add scheduler log files
    res["hpc_log"] = res.index.map(lambda x: Path(x[0]) / f"slurm-{x[1]}.out")
    # Remove log file for PENDING jobs
    res.loc[res.hpc_status.eq("PENDING"), "hpc_log"] = pd.NA
    # Remove missing log files
    res = res.loc[res["hpc_log"].map(lambda x: True if pd.isna(x) else x.exists())]
    # Reset index to Job Name
    res = res.reset_index().set_index("name")
    # Keep only most recent job
    res = res[~res.index.duplicated(keep="last")]

    # Join HPC info
    df = df.join(res)

    # Fill in missing status
    df["status"] = df["status"].fillna(df["hpc_status"])
    df["hpc_status"] = df["hpc_status"].fillna(df["status"])
    # PENDING HPC status takes precedence over other
    df.loc[df["hpc_status"].eq("PENDING"), "status"] = "PENDING"
    # RUNNING HPC status takes precedence over other
    df.loc[df["hpc_status"].eq("RUNNING"), "status"] = "RUNNING"

    # Check if status match
    status_match = df.apply(
        lambda row: row.status.startswith(row.hpc_status)
        or row.hpc_status.startswith(row.status),
        axis=1,
    )
    assert status_match.any(), f"Status values do not match:\n{df[~status_match]}"
else:
    logging.error(f"HPC scheduler {args.scheduler} not supported!")
    exit(1)
logging.debug(f"\n{df}")


assert (
    not df.status.isna().any()
), f"Missing status values:\n{df[df.status.isna()].iloc[0]}"


# Sort by status and remove duplicates
df["status"] = df["status"].fillna("UNKNOWN")
df["status"] = pd.Categorical(df["status"], sorted(set(status_msgs.values())))
df = df.sort_values("status")
df = df[~df.index.duplicated()]

# Print summary
df["total_samples"] = df.groupby("filename").status.transform("count")
df_status = df.groupby(["filename", "total_samples"]).status.value_counts(sort=False)
logging.info(f"\n{df_status[df_status > 0]}")

assert (
    n_jobs == df_status.sum()
), f"Number of jobs ({n_jobs}) and status ({df_status.sum()}) does not match"

# Filter runs by status and print
if args.status:
    for id in df[df.status.isin(args.status)].index:
        print(id)

exit(0)
