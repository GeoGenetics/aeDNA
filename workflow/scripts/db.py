import logging
import json
import copy
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
    logging.debug(f"Deleting report {report_id} from table 'plot_data'.")
    session.query(PlotData).filter(PlotData.report_id == report_id).delete()
    session.commit()
    # Delete plot category
    logging.debug(f"Deleting report {report_id} from table 'plot_category'.")
    session.query(PlotCategory).filter(PlotCategory.report_id == report_id).delete()
    session.commit()
    # Delete plot config
    logging.debug(f"Deleting report {report_id} from table 'plot_config'.")
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
    logging.debug(f"Deleting report {report_id} from table 'sample_data'.")
    session.query(SampleData).filter(SampleData.report_id == report_id).delete()
    session.commit()
    # Delete sample data type
    logging.debug(f"Deleting report {report_id} from table 'sample_data_type'.")
    session.query(SampleDataType).filter(
        SampleDataType.sample_data_type_id.in_(
            session.query(SampleDataType.sample_data_type_id)
            .outerjoin(SampleData)
            .filter(SampleData.sample_data_id == None)
        )
    ).delete(synchronize_session="fetch")
    session.commit()
    # Delete report metadata
    logging.debug(f"Deleting report {report_id} from table 'report_meta'.")
    session.query(ReportMeta).filter(ReportMeta.report_id == report_id).delete()
    session.commit()
    # Delete sample
    logging.debug(f"Deleting report {report_id} from table 'sample'.")
    session.query(Sample).filter(Sample.report_id == report_id).delete()
    session.commit()
    # Delete report
    logging.debug(f"Deleting report {report_id} from table 'report'.")
    session.query(Report).filter(Report.report_id == report_id).delete()
    session.commit()
    session.expunge_all()


def upload_report(engine, report_data, force=False):
    from sqlalchemy.orm import Session

    ### Create DB
    Base.metadata.create_all(engine)

    ### Open session
    with Session(engine) as session:
        import getpass
        from dateutil import parser

        ### Check if report exists by hash
        report_exists = (
            session.query(Report)
            .filter(Report.report_hash == report_data["config_report_hash"])
            .first()
        )
        if report_exists:
            logging.warning(
                f"Report {report_exists.report_id} in DB has the same hash ({report_data['config_report_hash']}). Skipping..."
            )
            return None
        ### Check if report exists by output folder
        report_exists = (
            session.query(ReportMeta)
            .filter(
                ReportMeta.report_meta_key == "config_output_dir",
                ReportMeta.report_meta_value == report_data["config_output_dir"],
            )
            .first()
        )

        if report_exists:
            logging.warning(
                f"Report {report_exists.report_id} in DB has the same output dir ({report_data['config_output_dir']})."
            )
            report_exists_older = (
                session.query(ReportMeta)
                .filter(
                    ReportMeta.report_id == report_exists.report_id,
                    ReportMeta.report_meta_key == "report_creation_date",
                    ReportMeta.report_meta_value < report_data["report_creation_date"],
                )
                .first()
            )
            if force:
                logging.warning(f"Force overwriting report {report_exists.report_id}.")
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

        ### Add report record
        logging.info("Adding report record to DB")
        report_record = Report(
            report_hash=report_data["config_report_hash"],
            created_at=parser.parse(report_data["report_creation_date"]),
        )
        session.add(report_record)
        session.commit()
        report_id = report_record.report_id

        ### Add report meta data
        logging.info("Adding report metadata to DB")
        # Add user name
        report_meta = ReportMeta(
            report_meta_key="username",
            report_meta_value=getpass.getuser(),
            report_id=report_id,
        )
        session.add(report_meta)
        # Add config info
        for key, value in report_data.items():
            if (
                key.startswith("config_")
                and not isinstance(value, list)
                and not isinstance(value, dict)
                and value
            ):
                report_meta = ReportMeta(
                    report_meta_key=key,
                    report_meta_value=value,
                    report_id=report_id,
                )
                session.add(report_meta)
        session.commit()

        ### Add RAW data to DB
        for section_key, section_data in report_data.get(
            "report_saved_raw_data", {}
        ).items():
            logging.info(f"Parsing section {section_key}")
            section_name = section_key.replace("multiqc_", "")
            for sample_key, sample_data in section_data.items():
                # Check if sample exists
                report_sample = (
                    session.query(Sample)
                    .filter(
                        Sample.sample_name == sample_key, Sample.report_id == report_id
                    )
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
        for plot_id, plot_data in report_data.get("report_plot_data", {}).items():
            logging.info(f"Parsing plot {plot_id}")
            #  skip custom plots
            if plot_id.startswith("mqc_hcplot_"):
                logging.warning("Skipping custom plot")
                continue
            if plot_data["plot_type"] not in ["bar plot", "x/y line"]:
                logging.warning(f"Plot type {plot_data['plot_type']} is not supported")
                continue
            plot_config = copy.deepcopy(plot_data.get("config", plot_data["pconfig"]))

            for dst_idx, dataset in enumerate(plot_data["datasets"]):
                dataset_id = dataset["uid"]
                logging.info(f"Parsing dataset {dataset_id}")
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

                plot_config_record = (
                    session.query(PlotConfig)
                    .filter(
                        PlotConfig.config_type == plot_data["plot_type"],
                        PlotConfig.config_name == dataset_id,
                        PlotConfig.config_dataset == dataset_name,
                    )
                    .first()
                )
                if plot_config_record:
                    logging.debug("Plot config already exists in DB")
                else:
                    logging.debug("Adding plot config to DB")
                    plot_config_record = PlotConfig(
                        config_type=plot_data["plot_type"],
                        config_name=dataset_id,
                        config_dataset=dataset_name,
                        data=json.dumps(plot_config),
                    )
                    session.add(plot_config_record)
                    session.commit()
                plot_config_id = plot_config_record.config_id

                # Save bar graph data
                if plot_data["plot_type"] == "bar plot":
                    for cat_data in dataset["cats"]:
                        data_key = str(cat_data["name"])
                        plot_category = (
                            session.query(PlotCategory)
                            .filter(
                                PlotCategory.report_id == report_id,
                                PlotCategory.config_id == plot_config_id,
                                PlotCategory.category_name == data_key,
                            )
                            .first()
                        )
                        data = json.dumps(
                            {
                                x: y
                                for x, y in list(cat_data.items())
                                if x not in ["data", "data_pct"]
                            }
                        )
                        if plot_category:
                            logging.debug("Plot category already exists in DB")
                            plot_category.data = data
                        else:
                            logging.debug("Adding plot category to DB")
                            plot_category = PlotCategory(
                                report_id=report_id,
                                config_id=plot_config_id,
                                category_name=data_key,
                                data=data,
                            )
                        session.add(plot_category)
                        session.commit()
                        plot_category_id = plot_category.plot_category_id

                        for sample_name, sample_data in zip(
                            dataset["samples"], cat_data["data"]
                        ):
                            sample = (
                                session.query(Sample)
                                .filter(
                                    Sample.sample_name == sample_name,
                                    Sample.report_id == report_id,
                                )
                                .first()
                            )
                            if sample:
                                logging.debug(
                                    f"Sample {sample_name} already exists in DB"
                                )
                            else:
                                logging.debug(f"Adding sample {sample_name} to DB")
                                sample = Sample(
                                    sample_name=sample_name, report_id=report_id
                                )
                                session.add(sample)
                                session.commit()
                            sample_id = sample.sample_id

                            plot_data_record = PlotData(
                                report_id=report_id,
                                config_id=plot_config_id,
                                sample_id=sample_id,
                                plot_category_id=plot_category_id,
                                data=json.dumps(sample_data),
                            )
                            session.add(plot_data_record)
                            session.commit()

                # Save line plot data
                elif plot_data["plot_type"] == "x/y line":
                    for line_data in dataset["lines"]:
                        try:
                            data_key = plot_config["data_labels"][dst_idx]["ylab"]
                        except (KeyError, TypeError, IndexError):
                            try:
                                data_key = plot_config["ylab"]
                            except KeyError:
                                data_key = plot_config["title"]

                        plot_category = (
                            session.query(PlotCategory)
                            .filter(
                                PlotCategory.report_id == report_id,
                                PlotCategory.config_id == plot_config_id,
                                PlotCategory.category_name == data_key,
                            )
                            .first()
                        )
                        data = json.dumps(
                            {
                                x: y
                                for x, y in list(line_data.items())
                                if x not in ["data", "pairs"]
                            }
                        )
                        if plot_category:
                            logging.debug("Plot category already exists in DB")
                            plot_category.data = data
                        else:
                            logging.debug("Adding plot category to DB")
                            plot_category = PlotCategory(
                                report_id=report_id,
                                config_id=plot_config_id,
                                category_name=data_key,
                                data=data,
                            )
                        session.add(plot_category)
                        session.commit()
                        plot_category_id = plot_category.plot_category_id

                        sample_name = line_data["name"]
                        sample = (
                            session.query(Sample)
                            .filter(
                                Sample.sample_name == sample_name,
                                Sample.report_id == report_id,
                            )
                            .first()
                        )
                        if sample:
                            logging.debug(f"Sample {sample_name} already exists in DB")
                        else:
                            logging.debug(f"Adding sample {sample_name} to DB")
                            sample = Sample(
                                sample_name=sample_name, report_id=report_id
                            )
                            session.add(sample)
                            session.commit()
                        sample_id = sample.sample_id

                        plot_data_record = PlotData(
                            report_id=report_id,
                            config_id=plot_config_id,
                            sample_id=sample_id,
                            plot_category_id=plot_category_id,
                            data=json.dumps(line_data["pairs"]),
                        )
                        session.add(plot_data_record)
                        session.commit()
