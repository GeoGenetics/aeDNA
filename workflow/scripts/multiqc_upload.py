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
        data.pop("report_creation_date")
        data_string = json.dumps(data, sort_keys=True).encode("utf-8")
        md5er = md5()
        md5er.update(data_string)
        ret = md5er.hexdigest()
        return ret

    if json_path.suffix.endswith(".zip"):
        from zipfile import ZipFile

        with ZipFile(json_path) as zfh:
            with zfh.open("multiqc_data.json") as fh:
                report = json.load(fh)
    elif json_path.suffix.endswith(".json"):
        with open(json_path, "r") as fh:
            report = json.load(fh)

    # Add report hash
    report["config_report_hash"] = generate_hash(report)
    # Add report output dir
    if not report.get("config_output_dir"):
        report["config_output_dir"] = json_path.absolute().parent
    logging.debug(report)
    return report


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
        "--db-upload",
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
        "--db-create",
        action="store_true",
        default=False,
        help="Create DB and exit.",
    )
    parser.add_argument(
        "--db-delete",
        action="store_true",
        default=False,
        help="Delete DB and exit.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Overwrite existing records?",
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

    # Create SQLAlchemy engine
    from sqlalchemy import create_engine
    from sqlalchemy_utils import database_exists, create_database, drop_database

    engine = create_engine(args.db_url)

    # Checking if DB exists
    if database_exists(engine.url):
        logging.info(f"DB {engine.url} exists.")
        # Deleting DB
        if args.db_delete:
            logging.warning(f"Deleting DB {engine.url}!")
            drop_database(engine.url)
            exit(0)
    else:
        logging.info(f"DB {engine.url} does not exist.")
        # Creating DB
        if args.db_create:
            logging.warning(f"Creating DB {engine.url}!")
            create_database(engine.url)
            exit(0)

    for file_upload in args.db_upload:
        # Load MultiQC JSON files
        logging.info(f"Reading file {file_upload}")
        report = load_multiqc(file_upload)
        logging.debug(report)

        # Upload to DB
        from db import upload_report

        logging.info(f"Uploading report v{report['config_version']} to DB...")
        upload_report(engine, report, args.force)


if __name__ == "__main__":
    main()
    exit(0)
