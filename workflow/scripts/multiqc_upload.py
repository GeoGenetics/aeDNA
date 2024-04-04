import logging
from collections import defaultdict

logging.basicConfig(
    encoding="utf-8",
    format="[%(asctime)s]:%(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def merge_dicts(dict1, dict2):
    return {
        key: {**dict1.get(key, {}), **dict2.get(key, {})}
        for key in set(dict1) | set(dict2)
    }


def parse_general(parsed):
    data = defaultdict(lambda: defaultdict(dict))
    for tool in parsed["report_general_stats_data"]:
        for sample, sample_data in tool.items():
            for key, value in sample_data.items():
                data[sample]["general"][key] = value
    return data


def parse_raw(parsed):
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    for tool, tool_data in parsed["report_saved_raw_data"].items():
        for sample, sample_data in tool_data.items():
            for key, value in sample_data.items():
                data[sample]["raw"][tool.replace("multiqc_", "")][key] = value
    return data


def parse_metadata(parsed, samples, metadata_func):
    data = defaultdict(lambda: defaultdict(dict))
    for sample in samples:
        for key, value in metadata_func(sample, parsed):
            data[sample]["metadata"][key] = value
    return data


def load_multiqc(
    paths,
    sections=None,
    metadata_keys=None,
    metadata_func=None,
    plots=None,
    plot_parsers=None,
):
    import json

    def generate_hash(d):
        import copy
        from hashlib import md5

        data = copy.deepcopy(d)
        data["metadata"].pop("config_creation_date")
        data_string = json.dumps(data, sort_keys=True).encode("utf-8")
        md5er = md5()
        md5er.update(data_string)
        ret = md5er.hexdigest()
        return ret

    reports = []
    for json_path in paths:
        data = dict()
        logging.info(f"Reading file {json_path}")
        if json_path.suffix.endswith(".zip"):
            from zipfile import ZipFile

            with ZipFile(json_path) as zfh:
                with zfh.open("multiqc_data.json") as fh:
                    parsed = json.load(fh)
        elif json_path.suffix.endswith(".json"):
            with open(json_path, "r") as fh:
                parsed = json.load(fh)

        for section in sections:
            if section == "general":
                logging.info(f"Parsing '{section}' section")
                data = merge_dicts(data, parse_general(parsed))
            elif section == "raw":
                logging.info(f"Parsing '{section}' section")
                data = merge_dicts(data, parse_raw(parsed))
            elif section == "plot":
                logging.info(f"Parsing '{section}' section")
                data = merge_dicts(
                    data, parse_plots(parsed, plots=plots, plot_parsers=plot_parsers)
                )

        data = merge_dicts(
            data,
            parse_metadata(parsed, samples=data.keys(), metadata_func=metadata_func),
        )
        data = {
            "data": data,
            "metadata": {key: parsed.get(key) for key in metadata_keys},
        }
        data["metadata"]["report_hash"] = generate_hash(data)
        reports.append(data)

    logging.debug(reports)
    return reports


############
### MAIN ###
############
def main():
    import argparse
    from pathlib import Path

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Parse MultiQC JSON files",
        allow_abbrev=False,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--json",
        action="store",
        type=Path,
        nargs="+",
        help="Paths of MultiQC JSON files.",
    )
    parser.add_argument(
        "-s",
        "--sections",
        action="store",
        nargs="+",
        default=["raw"],
        choices=["general", "raw", "plot"],
        help="Output file name.",
    )
    parser.add_argument(
        "--metadata_keys",
        action="store",
        nargs="+",
        default=["config_creation_date", "config_intro_text", "config_output_dir", "config_script_path", "config_short_version", "config_subtitle", "config_version"],
        help="JSON keys to add as report metadata.",
    )
    parser.add_argument(
        "--metadata_func",
        action="store",
        default=lambda x, y: {},
        help="Function to extract sample metadata.",
    )
    parser.add_argument(
        "--db-create",
        action="store_true",
        default=False,
        help="Create (and overwrite) DB?.",
    )
    parser.add_argument(
        "--db-url",
        action="store",
        default="sqlite:///test.sqlite",
        help="DB connection.",
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

    # Load MultiQC JSON files
    reports = load_multiqc(
        args.json,
        metadata_keys=args.metadata_keys,
        metadata_func=args.metadata_func,
        sections=args.sections,
    )
    logging.debug(reports)

    # Upload to DB
    from db import upload_reports
    upload_reports(args.db_url, reports)


if __name__ == "__main__":
    main()
