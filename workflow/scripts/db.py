
import logging


def upload_reports (db_url, reports):
    from sqlalchemy import create_engine
    from sqlalchemy_utils import database_exists
    from sqlalchemy.orm import Session


    # Create SQLAlchemy emgine
    engine = create_engine(db_url)


    # Check if DB exists
    if database_exists(engine.url):
        logging.info(f"DB {engine.url} exists.")
    else:
        # Create DB
        from models import Base
        Base.metadata.create_all(engine)


    # Open session
    with Session(engine) as session:
        import datetime
        import pandas as pd
        from models import Report, Sample, SampleDataType, SampleData

        for report in reports:
            # Check if report exists
            if (
                session.query(Report)
                .filter(Report.report_hash == report["metadata"]["report_hash"])
	        .first()
            ):
                logging.info("Report already exists in DB. Skipping...")
                continue

            # Add to main report table
            new_report = Report(
                report_hash=report["metadata"].pop("report_hash"),
	        created_at=datetime.datetime.strptime(
                    report["metadata"]["config_creation_date"], "%Y-%m-%d, %H:%M %Z"
                ),
            )
            session.add(new_report)
            session.commit()
            report_id = new_report.report_id

            # Add to report metadata table
            df = pd.DataFrame.from_dict(report["metadata"], orient="index").reset_index()
            df.columns = ["report_meta_key", "report_meta_value"]
            df["report_id"] = report_id
            df.to_sql(name="report_meta", con=engine, if_exists="append", index=False)

            # Add samples
            for sample_id, sample_data in report["data"].items():
                # Check if sample exists
                report_sample = (
                    session.query(Sample).filter(Sample.sample_name == sample_id).first()
                )
                if not report_sample:
                    report_sample = Sample(sample_name=sample_id, report_id=report_id)
                    session.add(report_sample)
                    session.commit()
                sample_id = report_sample.sample_id

                if not sample_data.get("raw"):
                    logging.info(f"RAW data section not found for sample {sample_id}. Skipping...")
                    continue

                # Go through each data key
                for section_id, section_data in sample_data["raw"].items():
                    for data_id, data_value in section_data.items():
                        # Save / load the data type
                        key_type = (
                            session.query(SampleDataType)
                            .filter(SampleDataType.data_id == data_id and SampleDataType.data_section == section_id)
                            .first()
                        )
                        if not key_type:
                            key_type = SampleDataType(
                                data_id=data_id,
                                data_section=section_id,
                                data_key=f"{section_id}__{data_id}",
                            )
                            session.add(key_type)
                            session.commit()
                        type_id = key_type.sample_data_type_id

                        # Save the data value
                        new_data = SampleData(
                            report_id=report_id,
                            sample_data_type_id=type_id,
                            sample_id=sample_id,
                            value=str(data_value),
                        )
                        session.add(new_data)
                        session.commit()
