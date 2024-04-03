#!/usr/bin/env python3
#
# Copyright Filipe G. Vieira (2021)
import logging

logging.basicConfig(
    encoding="utf-8",
    format="[%(asctime)s]:%(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def flatten(list_of_lists: list) -> list:
    """Flatten a list of lists recursively

    https://stackoverflow.com/a/53778278

    :param list_of_lists: A list of lists
    :return result: A string that has been flattened from a list of lists
    """
    result = list()
    for i in list_of_lists:
        if isinstance(i, list):
            result.extend(flatten(i))
        else:
            result.append(str(i))
    return result


# Parse log data from JSON file
# {"benchmark":"benchmarks/align/sort_name/CGG3-017623_LV7001884343-LV7001308305-CTTATTGGCCxACTTGTTATC_collapsed.norway.tsv","indent":false,"input":["temp/align/merge_alns/CGG3-017623_LV7001884343-LV7001308305-CTTATTGGCCxACTTGTTATC_collapsed.norway.sam"],"is_checkpoint":false,"is_handover":false,"jobid":4163,"level":"job_info","local":false,"log":["logs/align/sort_name/CGG3-017623_LV7001884343-LV7001308305-CTTATTGGCCxACTTGTTATC_collapsed.norway.log"],"msg":null,"name":"taxon_sort_name","output":["results/CGG3-017623_LV7001884343-LV7001308305-CTTATTGGCCxACTTGTTATC_collapsed.norway.sam"],"printshellcmd":true,"priority":0,"reason":"Missing output files: results/CGG3-017623_LV7001884343-LV7001308305-CTTATTGGCCxACTTGTTATC_collapsed.norway.sam","resources":[4,1,307200,292969,1340668,1278561,"<TBD>","gpuqueue,comppriority",600],"threads":4,"timestamp":1683447988.9035554,"wildcards":{"library":"LV7001884343-LV7001308305-CTTATTGGCCxACTTGTTATC","read_type_map":"collapsed","ref":"norway","sample":"CGG3-017623"}}

# {"jobid":4252,"level":"job_finished","timestamp":1683448666.3186321}

# {"aux":{},"conda_env":"/home/lnc113/.cache/snakemake/conda/1a6a76a34b2c6ada076c05677e46730b_","indent":false,"input":["temp/reads/repr_read/grep/CGG3-017647_LV7001884364-LV7001308368-AGCCTATGATxGTAGGTGGTG_collapsed.vsearch.fastq.gz"],"jobid":2419,"level":"job_error","log":["logs/reads/nonpareil/CGG3-017647_LV7001884364-LV7001308368-AGCCTATGATxGTAGGTGGTG_collapsed.vsearch.log",".snakemake/slurm_logs/rule_derep_nonpareil/1373032.log"],"msg":"SLURM-job '1373032' failed, SLURM status is: 'FAILED'","name":"derep_nonpareil","output":["stats/reads/nonpareil/CGG3-017647_LV7001884364-LV7001308368-AGCCTATGATxGTAGGTGGTG_collapsed.vsearch.npo","stats/reads/nonpareil/CGG3-017647_LV7001884364-LV7001308368-AGCCTATGATxGTAGGTGGTG_collapsed.vsearch.npa","stats/reads/nonpareil/CGG3-017647_LV7001884364-LV7001308368-AGCCTATGATxGTAGGTGGTG_collapsed.vsearch.npc"],"shellcmd":null,"timestamp":1683449697.937108}


def merge_jobs(job1, job2):
    from datetime import datetime

    status = job2.pop("level")
    timestamp = job2.pop("timestamp")
    timestamp_posix = str(datetime.fromtimestamp(timestamp))
    reason = job2.pop("reason", None)

    # Merge jobs
    job = job1 | job2

    # Add status info
    status_info = dict()
    for k in ["status", "timestamp", "timestamp_posix", "reason"]:
        if eval(k):
            status_info[k] = eval(k)
    # Add status to job info
    job["status"][timestamp] = status_info

    return job


def parse_snakemake_log_json(snakemake_log):
    import json
    from collections import defaultdict

    jobs = defaultdict(lambda: defaultdict(dict))

    # Open LOG file
    log_file = open(snakemake_log)

    for line in log_file:
        log_entry = json.loads(line)

        # Restrict entry types to parse
        if log_entry["level"] not in ["job_info", "job_finished", "job_error"]:
            continue

        # Add labels to resources
        if "resources" in log_entry:
            del log_entry["resources"][7:-1]
            log_entry["resources"] = dict(
                zip(
                    [
                        "cores",
                        "nodes",
                        "mem_mb",
                        "mem_mib",
                        "disk_mb",
                        "disk_mib",
                        "tmp_dir",
                        "runtime",
                    ],
                    log_entry["resources"],
                )
            )

        # Merge job info
        jobid = log_entry["jobid"]
        jobs[jobid] = merge_jobs(jobs[jobid], log_entry)

    # Close log fh
    log_file.close()

    return jobs


def rename_keys(d, keys):
    new_dict = list()
    for _, v in d.items():
        k = "_".join(
            flatten(
                [
                    (
                        list(dict(sorted(v[key].items())).values())
                        if isinstance(v[key], dict)
                        else v[key]
                    )
                    for key in keys
                ]
            )
        )
        new_dict.append((k, v))

    return dict(new_dict)


def parse_snakemake_log_dir(snakemake_logs):
    from collections import defaultdict

    jobs_all = defaultdict(dict)

    for log in sorted(snakemake_logs.glob("*.json")):
        logging.debug(f"\tParsing file {log} ...")
        for jobid, job in rename_keys(
            parse_snakemake_log_json(log), ["name", "wildcards"]
        ).items():
            jobs_all[jobid] = jobs_all[jobid] | job

    return jobs_all


############
### Main ###
############
def main():
    import argparse
    from pathlib import Path
    import json
    from collections import defaultdict, Counter

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Generic snakemake wrapper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-s",
        "--logs",
        "--snake_logs",
        "--snakemake_logs",
        action="store",
        dest="snakemake_logs",
        type=Path,
        default=".snakemake/log",
        help="Path to Snakemake LOG file/folder.",
    )
    parser.add_argument(
        "-i",
        "--verbose_info",
        action="count",
        dest="job_info",
        default=0,
        help="Verbose info on submitted jobs.",
    )
    #    parser.add_argument("-r", "--verbose_running", action="count", dest="job_run", default=0, help="Verbose info on running jobs.")
    parser.add_argument(
        "-f",
        "--verbose_finished",
        action="count",
        dest="job_finished",
        default=0,
        help="Verbose info on finished jobs.",
    )
    parser.add_argument(
        "-e",
        "--verbose_errors",
        action="count",
        dest="job_error",
        default=0,
        help="Verbose info errors.",
    )
    parser.add_argument(
        "--out_json", action="store", type=Path, help="Output parsed LOG files as JSON."
    )
    parser.add_argument(
        "-l",
        "--log-level",
        action="store",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity level",
    )
    args = parser.parse_args()

    # LOG
    logging.getLogger().setLevel(args.log_level.upper())
    logging.debug(args)

    # Parse Snakemake's LOG
    if args.snakemake_logs.exists():
        if args.snakemake_logs.is_dir():
            logging.debug(f"Parsing all LOGs in folder ({args.snakemake_logs}) ...")
            jobs = parse_snakemake_log_dir(args.snakemake_logs)
        else:
            logging.debug(f"Parsing LOG file ({args.snakemake_logs}) ...")
            jobs = parse_snakemake_log_json(args.snakemake_logs)
    else:
        raise ValueError("LOG file/folder does not exist!")
    logging.debug(jobs)

    # Output JSON
    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(jobs, f, ensure_ascii=True, indent=4)

    # Build status dict
    status = defaultdict(lambda: defaultdict(list))
    for jobid, job in jobs.items():
        for timestamp, entry in sorted(
            job["status"].items(), key=lambda x: x[1]["timestamp"]
        ):
            # If a new job, remove previous errors
            if entry["status"] == "job_info" and jobid in status["job_error"]:
                status["job_info"].pop(jobid)
                status["job_error"].pop(jobid)
            # Add entry
            status[entry["status"]][jobid].append(entry["timestamp_posix"])
    # Clean empty keys
    status = {key: val for key, val in status.items() if val}
    logging.debug(status)

    # Print summary
    for level, jobids in status.items():
        print(f"{level}: {args.snakemake_logs.absolute()}")
        if getattr(args, level) > 0:
            logging.info(f"{level}: {len(jobids)}")

        if getattr(args, level) > 1:
            for name, cnt in Counter([jobs[jobid]["name"] for jobid in jobids]).items():
                logging.info(f"\t{name}: {cnt}")

                if getattr(args, level) > 2:
                    for jobid in jobids:
                        if jobs[jobid]["name"] == name:
                            logging.info(f"\t\t{jobid}: {len(jobids[jobid])}")

                            if getattr(args, level) > 3:
                                logging.info("\t\t\t".join([""] + jobids[jobid]))


if __name__ == "__main__":
    main()
