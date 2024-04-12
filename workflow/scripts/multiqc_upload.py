import logging
from collections import defaultdict

logging.basicConfig(
    encoding="utf-8",
    format="[%(asctime)s]:%(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def load_multiqc(json_path):
    """
    Loads one MultiQC report (.json or .zip) into a dict.
    @param json_path A string vector of file path
    @export
    @return A dict() with MultiQC JSON
    """
    import json

    def generate_hash(d):
        import copy
        from hashlib import md5

        data = copy.deepcopy(d)
        data.pop("config_creation_date")
        data_string = json.dumps(data, sort_keys=True).encode("utf-8")
        md5er = md5()
        md5er.update(data_string)
        ret = md5er.hexdigest()
        return ret

    if json_path.suffix.endswith(".zip"):
        from zipfile import ZipFile

        with ZipFile(json_path) as zfh:
            with zfh.open("multiqc_data.json") as fh:
                parsed = json.load(fh)
    elif json_path.suffix.endswith(".json"):
        with open(json_path, "r") as fh:
            parsed = json.load(fh)

    parsed["config_report_hash"] = generate_hash(parsed)
    if not parsed.get("config_output_dir"):
        parsed["config_output_dir"] = json_path.absolute().parent
    logging.debug(parsed)
    return parsed


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
        "--db-url",
        action="store",
        default="sqlite:///test.sqlite",
        help="DB connection.",
    )
    parser.add_argument(
        "--db-force",
        action="store_true",
        default=False,
        help="Overwrite DB (if exists)?.",
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

    for json_path in args.json:
        # Load MultiQC JSON files
        logging.info(f"Reading file {json_path}")
        report = load_multiqc(json_path)
        logging.debug(report)

        # Upload to DB
        from db import upload_report

        logging.info(f"Uploading report to {args.db_url}")
        upload_report(args.db_url, report, args.db_force)


if __name__ == "__main__":
    main()
