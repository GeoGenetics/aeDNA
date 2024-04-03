import json
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    encoding="utf-8",
    format="[%(asctime)s]:%(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

ROW_IDENTIFIER = "metadata.sample_id"


def parse_general(parsed):
    df = pd.concat(
        [
            pd.DataFrame.from_dict(tool, orient="index")
            for tool in parsed["report_general_stats_data"]
        ]
    )
    df.columns = "general." + df.columns
    return df


def parse_raw(parsed):
    samples = []
    for tool, tool_data in parsed["report_saved_raw_data"].items():
        tool = tool.replace("multiqc_", "")
        df = pd.DataFrame.from_dict(tool_data, orient="index")
        df.columns = f"raw.{tool}." + df.columns
        samples.append(df)
    return pd.concat(samples)


def parse_metadata(parsed, samples, metadata_func):
    df = pd.concat(
        [
            pd.DataFrame.from_dict(
                {sample: metadata_func(sample, parsed)}, orient="index"
            )
            for sample in samples
        ]
    )
    df.columns = "metadata." + df.columns
    return df


def load_multiqc(
    paths, plots=None, metadata_func=lambda x: {}, plot_parsers=None, sections=None
):
    data_frames = []
    for json_path in paths:
        logging.info(f"Reading file {json_path}")
        if json_path.suffix.endswith(".zip"):
            from zipfile import ZipFile
            with ZipFile(json_path) as zfh:
                with zfh.open("multiqc_data.json") as fh:
                    parsed = json.load(fh)
        elif json_path.suffix.endswith(".json"):
            with open(json_path, "r") as fh:
                parsed = json.load(fh)

        df = pd.DataFrame()
        for section in sections:
            if section == "general":
                logging.info(f"Parsing '{section}' section")
                df = df.join(parse_general(parsed), how="outer")
            elif section == "raw":
                logging.info(f"Parsing '{section}' section")
                df = df.join(parse_raw(parsed), how="outer")
            elif section == "plot":
                logging.info(f"Parsing '{section}' section")
                df = df.join(
                    parse_plots(parsed, plots=plots, plot_parsers=plot_parsers),
                    how="outer",
                )

        df = df.join(
            parse_metadata(
                parsed,
                samples=df.index.drop_duplicates().values,
                metadata_func=metadata_func,
            ),
            how="outer",
        )
        data_frames.append(df)

    return pd.concat(data_frames)


############
### MAIN ###
############
def main():
    import argparse

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
        default=["/home/lnc113/XXX/multiqc_data.json"],
        help="Paths of MultiQC JSON files.",
    )
    parser.add_argument(
        "-s",
        "--sections",
        action="store",
        nargs="+",
        default=["general"],
        choices=["general", "raw", "plot"],
        help="Output file name.",
    )
    parser.add_argument(
        "--metadata_keys",
        action="store",
        nargs="+",
        default=["config_creation_date", "config_version", "config_output_dir"],
        help="JSON keys to add as metadata.",
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
    df = load_multiqc(
        args.json,
        metadata_func=lambda sample, parsed: {
            key: parsed[key] for key in args.metadata_keys
        },
        sections=args.sections,
    )
    logging.debug(df)


if __name__ == "__main__":
    main()
