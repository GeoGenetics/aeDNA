import logging
import json
import copy
from packaging import version
from sqlalchemy import select, delete
from sqlalchemy.sql import and_, not_, or_

from models import (
    Base,
    Report,
    ReportMeta,
    PlotConfig,
    PlotData,
    PlotCategory,
    SampleDataType,
    SampleData,
    Sample,
)


def delete_report(session, report_id):
    # Delete plot data
    logging.debug(f"Deleting report {report.id} from table 'plot_data'.")
    session.query(PlotData).filter(PlotData.report_id == report_id).delete()
    session.commit()
    # Delete plot category
    logging.debug(f"Deleting report {report.id} from table 'plot_category'.")
    session.query(PlotCategory).filter(PlotCategory.report_id == report_id).delete()
    session.commit()
    # Delete plot config
    logging.debug(f"Deleting report {report.id} from table 'plot_config'.")
    session.query(PlotConfig).filter(
        PlotConfig.config_id.in_(
            session.query(PlotConfig.config_id)
            .outerjoin(PlotData)
            .outerjoin(PlotCategory, PlotCategory.config_id == PlotConfig.config_id)
            .filter(
                and_(
                    PlotData.plot_data_id == None, PlotCategory.plot_category_id == None
                )
            )
        )
    ).delete(synchronize_session="fetch")
    session.commit()
    # Delete sample data
    logging.debug(f"Deleting report {report.id} from table 'sample_data'.")
    session.query(SampleData).filter(SampleData.report_id == report_id).delete()
    session.commit()
    # Delete sample data type
    logging.debug(f"Deleting report {report.id} from table 'sample_data_type'.")
    session.query(SampleDataType).filter(
        SampleDataType.sample_data_type_id.in_(
            session.query(SampleDataType.sample_data_type_id)
            .outerjoin(SampleData)
            .filter(SampleData.sample_data_id == None)
        )
    ).delete(synchronize_session="fetch")
    session.commit()
    # Delete report metadata
    logging.debug(f"Deleting report {report.id} from table 'report_meta'.")
    session.query(ReportMeta).filter(ReportMeta.report_id == report_id).delete()
    session.commit()
    # Delete sample
    logging.debug(f"Deleting report {report.id} from table 'sample'.")
    session.query(Sample).filter(Sample.report_id == report_id).delete()
    session.commit()
    # Delete report
    logging.debug(f"Deleting report {report.id} from table 'report'.")
    session.query(Report).filter(Report.report_id == report_id).delete()
    session.commit()


def upload_report(engine, report, force=False):
    from sqlalchemy.orm import Session

    # Create DB
    Base.metadata.create_all(engine)

    # Open session
    with Session(engine) as session:
        import getpass
        from dateutil import parser

        # Check if report exists by hash
        report_exists = (
            session.query(Report)
            .filter(Report.report_hash == report["config_report_hash"])
            .first()
        )
        if report_exists:
            logging.warning(
                f"Report {report_exists.report_id} in DB has the same hash ({report['config_report_hash']}). Skipping..."
            )
            return None
        # Check if report exists by output folder
        report_exists = (
            session.query(ReportMeta)
            .filter(
                ReportMeta.report_meta_key == "config_output_dir",
                ReportMeta.report_meta_value == report["config_output_dir"],
            )
            .first()
        )

        if report_exists:
            logging.warning(
                f"Report {report_exists.report_id} in DB has the same output dir ({report['config_output_dir']})."
            )
            report_exists_older = (
                session.query(ReportMeta)
                .filter(
                    ReportMeta.report_id == report_exists.report_id,
                    ReportMeta.report_meta_key == "report_creation_date",
                    ReportMeta.report_meta_value < report["report_creation_date"],
                )
                .first()
            )
            if force:
                logging.warning(
                    f"Force overwriting report {report_exists.report_id}."
                )
                delete_report(session, report_exists.report_id)
            else:
                if report_exists_older:
                    logging.warning(
                        f"Overwriting existing report {report_exists.report_id}, since it is older."
                    )
                    delete_report(session, report_exists.report_id)
                else:
                    logging.warning(
                        f"Keeping existing report {report_exists.report_id}, since it is newer."
                    )
                    return None

        new_report = Report(
            report_hash=report["config_report_hash"],
            created_at=parser.parse(report["report_creation_date"]),
        )
        session.add(new_report)
        session.commit()
        report_id = new_report.report_id

        logging.info("Adding report metadata to DB")
        # Add user name
        new_report_meta = ReportMeta(
            report_meta_key="username",
            report_meta_value=getpass.getuser(),
            report_id=report_id,
        )
        session.add(new_report_meta)
        # Add config info
        for key, value in report.items():
            if (
                key.startswith("config_")
                and not isinstance(value, list)
                and not isinstance(value, dict)
                and value
            ):
                new_report_meta = ReportMeta(
                    report_meta_key=key,
                    report_meta_value=value,
                    report_id=report_id,
                )
                session.add(new_report_meta)
        session.commit()

        # Add RAW data to DB
        for section_key, section_data in report.get(
            "report_saved_raw_data", {}
        ).items():
            logging.info(f"Parsing section {section_key}")
            section_name = section_key.replace("multiqc_", "")
            for sample_key, sample_data in section_data.items():
                # Check if sample exists
                report_sample = (
                    session.query(Sample)
                    .filter(Sample.sample_name == sample_key, Sample.report_id == report_id)
                    .first()
                )
                if report_sample:
                    logging.debug(f"Sample {sample_key} already exists in DB")
                else:
                    logging.debug(f"Adding sample {sample_key} to DB")
                    report_sample = Sample(sample_name=sample_key, report_id=report_id)
                    session.add(report_sample)
                    session.commit()
                sample_id = report_sample.sample_id

                # Go through each data key
                for data_key, data_value in sample_data.items():
                    # Save / load the data type
                    key_type = (
                        session.query(SampleDataType)
                        .filter(
                            SampleDataType.data_id == data_key,
                            SampleDataType.data_section == section_name,
                        )
                        .first()
                    )
                    if key_type:
                        logging.debug(
                            f"Sample data type {data_key} already exists in DB"
                        )
                    else:
                        logging.debug(f"Adding sample data type {data_key} to DB")
                        key_type = SampleDataType(
                            data_id=data_key,
                            data_section=section_name,
                            data_key=f"{section_name}__{data_key}",
                        )
                        session.add(key_type)
                        session.commit()
                    type_id = key_type.sample_data_type_id

                    # Save the data value
                    data_value = SampleData(
                        report_id=report_id,
                        sample_data_type_id=type_id,
                        sample_id=sample_id,
                        value=str(data_value),
                    )
                    session.add(data_value)
                    session.commit()

        # Add plot data to DB
        for plot_id, plot_data in report.get("report_plot_data", {}).items():
            logging.info(f"Parsing plot {plot_id}")
            #  skip custom plots
            if plot_id.startswith("mqc_hcplot_"):
                logging.warning("Skipping custom plot")
                continue
            if plot_data["plot_type"] not in ["bar_graph", "xy_line"]:
                logging.warning(f"Plot type {plot_data['plot_type']} is not supported")
                continue
            plot_config = copy.deepcopy(plot_data.get("config", plot_data["pconfig"]))

            for dst_idx, dataset in enumerate(plot_data["datasets"]):
                # MultiQC 1.20 stores "categories" per-dataset, so need to re-add it into
                # the main pconfig for MegaQC:
                dls = None
                dataset_name = None
                if "data_labels" in plot_config and dst_idx < len(
                    plot_config["data_labels"]
                ):
                    dls = plot_config["data_labels"][dst_idx]
                    if isinstance(dls, dict):
                        if "ylab" in dls:
                            dataset_name = dls["ylab"]
                        for k, v in dls.items():
                            plot_config[k] = v
                    else:
                        dataset_name = dls
                if not dataset_name:
                    dataset_name = plot_config.get("ylab")
                if not dataset_name:
                    dataset_name = plot_config.get("title")

                plot_configX = (
                    session.query(PlotConfig)
                    .filter(
                        PlotConfig.config_type == plot_data["plot_type"],
                        PlotConfig.config_name == plot_id,
                        PlotConfig.config_dataset == dataset_name,
                    )
                    .first()
                )
                if plot_configX:
                    logging.debug("Plot config already exists in DB")
                else:
                    logging.debug("Adding plot config to DB")
                    plot_configX = PlotConfig(
                        config_type=plot_data["plot_type"],
                        config_name=plot_id,
                        config_dataset=dataset_name,
                        data=json.dumps(plot_config),
                    )
                    session.add(plot_configX)
                    session.commit()
                plot_config_id = plot_configX.config_id

                # Save bar graph data
                if plot_data["plot_type"] == "bar_graph":
                    for cat_data in (
                        dataset
                        if version.parse(report["config_version"])
                        <= version.parse("1.19")
                        else dataset["cats"]
                    ):
                        data_key = str(cat_data["name"])
                        category = (
                            session.query(PlotCategory)
                            .filter(PlotCategory.report_id == report_id,
                                    PlotCategory.config_id == plot_config_id,
                                    PlotCategory.category_name == data_key)
                            .first()
                        )
                        data = json.dumps(
                            {
                                x: y
                                for x, y in list(cat_data.items())
                                if x not in ["data", "data_pct"]
                            }
                        )
                        if category:
                            logging.debug("Plot category already exists in DB")
                            category.data = data
                        else:
                            logging.debug("Adding plot category to DB")
                            category = PlotCategory(
                                report_id=report_id,
                                config_id=plot_config_id,
                                category_name=data_key,
                                data=data,
                            )
                        session.add(category)
                        session.commit()
                        plot_category_id = category.plot_category_id

                        for sname, actual_data in zip(
                            (
                                plot_data["samples"][dst_idx]
                                if version.parse(report["config_version"])
                                <= version.parse("1.19")
                                else dataset["samples"]
                            ),
                            cat_data["data"],
                        ):
                            sample = (
                                session.query(Sample)
                                .filter(Sample.sample_name == sname, Sample.report_id == report_id)
                                .first()
                            )
                            if sample:
                                logging.debug(f"Sample {sname} already exists in DB")
                            else:
                                sample = Sample(sample_name=sname, report_id=report_id)
                                session.add(sample)
                                session.commit()
                            sample_id = sample.sample_id

                            new_dataset_row = PlotData(
                                report_id=report_id,
                                config_id=plot_config_id,
                                sample_id=sample_id,
                                plot_category_id=plot_category_id,
                                data=json.dumps(actual_data),
                            )
                            session.add(new_dataset_row)
                            session.commit()

                # Save line plot data
                elif plot_data["plot_type"] == "xy_line":
                    for line_data in (
                        dataset
                        if version.parse(report["config_version"])
                        <= version.parse("1.19")
                        else dataset["lines"]
                    ):
                        try:
                            data_key = plot_config["data_labels"][dst_idx]["ylab"]
                        except (KeyError, TypeError, IndexError):
                            try:
                                data_key = plot_config["ylab"]
                            except KeyError:
                                data_key = plot_config["title"]

                        category = (
                            session.query(PlotCategory)
                            .filter(PlotCategory.report_id == report_id,
                                    PlotCategory.config_id == plot_config_id,
                                    PlotCategory.category_name == data_key)
                            .first()
                        )
                        data = json.dumps(
                            {x: y for x, y in list(line_data.items()) if x != "data"}
                        )
                        if category:
                            logging.debug("Plot category already exists in DB")
                            category.data = data
                        else:
                            logging.debug("Adding plot category to DB")
                            category = PlotCategory(
                                report_id=report_id,
                                config_id=plot_config_id,
                                category_name=data_key,
                                data=data,
                            )
                        session.add(category)
                        session.commit()
                        plot_category_id = category.plot_category_id

                        sample_name = line_data["name"]
                        sample = (
                            session.query(Sample)
                            .filter(Sample.sample_name == sample_name, Sample.report_id == report_id)
                            .first()
                        )
                        if sample:
                            logging.debug(f"Sample {sample_name} already exists in DB")
                        else:
                            logging.debug(f"Adding sample {sample_name} to DB")
                            sample = Sample(sample_name=sample_name, report_id=report_id)
                            session.add(sample)
                            session.commit()
                        sample_id = sample.sample_id

                        new_dataset_row = PlotData(
                            report_id=report_id,
                            config_id=plot_config_id,
                            sample_id=sample_id,
                            plot_category_id=plot_category_id,
                            data=json.dumps(line_data['pairs']),
                        )
                        session.add(new_dataset_row)
                        session.commit()
