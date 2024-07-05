# -*- coding: utf-8 -*-
import datetime
from sqlalchemy import Column, DateTime, ForeignKey, Integer, UnicodeText
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Report(Base):
    """
    A MultiQC report.
    """

    __tablename__ = "report"
    report_id = Column(Integer, primary_key=True)
    report_hash = Column(UnicodeText, nullable=False, index=True, unique=True)
    created_at = Column(
        DateTime, nullable=False, default=datetime.datetime.now().astimezone()
    )
    uploaded_at = Column(
        DateTime, nullable=False, default=datetime.datetime.now().astimezone()
    )
    meta = relationship("ReportMeta", back_populates="report", passive_deletes="all")
    samples = relationship("Sample", back_populates="report", passive_deletes="all")
    sample_data = relationship(
        "SampleData", back_populates="report", passive_deletes="all"
    )


class ReportMeta(Base):
    __tablename__ = "report_meta"
    report_meta_id = Column(Integer, primary_key=True)
    report_meta_key = Column(UnicodeText, nullable=False)
    report_meta_value = Column(UnicodeText, nullable=False)
    # If the report is deleted, remove the report metadata
    report_id = Column(
        Integer,
        ForeignKey("report.report_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    report = relationship("Report", back_populates="meta")


class PlotConfig(Base):
    __tablename__ = "plot_config"
    config_id = Column(Integer, primary_key=True)
    config_type = Column(UnicodeText, nullable=False)
    config_name = Column(UnicodeText, nullable=False)
    config_dataset = Column(UnicodeText, nullable=True)
    data = Column(UnicodeText, nullable=False)


class PlotData(Base):
    __tablename__ = "plot_data"
    plot_data_id = Column(Integer, primary_key=True)
    report_id = Column(Integer, ForeignKey("report.report_id"), index=True)
    config_id = Column(Integer, ForeignKey("plot_config.config_id"))
    plot_category_id = Column(Integer(), ForeignKey("plot_category.plot_category_id"))
    sample_id = Column(Integer, ForeignKey("sample.sample_id"), index=True)
    data = Column(UnicodeText, nullable=False)


class PlotCategory(Base):
    __tablename__ = "plot_category"
    plot_category_id = Column(Integer, primary_key=True)
    report_id = Column(Integer, ForeignKey("report.report_id"))
    config_id = Column(Integer, ForeignKey("plot_config.config_id"))
    category_name = Column(UnicodeText, nullable=True)
    data = Column(UnicodeText, nullable=False)


class SampleDataType(Base):
    __tablename__ = "sample_data_type"
    sample_data_type_id = Column(Integer, primary_key=True)
    data_id = Column(UnicodeText, nullable=False)
    data_section = Column(UnicodeText, nullable=False)
    data_key = Column(UnicodeText, nullable=False)
    schema = Column(
        UnicodeText,
        doc="A JSON Schema for validating and describing the data of this type",
    )
    sample_data = relationship("SampleData", back_populates="data_type")


class SampleData(Base):
    __tablename__ = "sample_data"
    sample_data_id = Column(Integer, primary_key=True)
    report_id = Column(Integer, ForeignKey("report.report_id"), index=True)
    sample_data_type_id = Column(
        Integer,
        ForeignKey("sample_data_type.sample_data_type_id", ondelete="CASCADE"),
        nullable=False,
    )
    sample_id = Column(
        Integer,
        ForeignKey("sample.sample_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    value = Column(UnicodeText)
    sample = relationship("Sample", back_populates="data")
    report = relationship("Report", back_populates="sample_data")
    data_type = relationship("SampleDataType", back_populates="sample_data")


class Sample(Base):
    __tablename__ = "sample"
    sample_id = Column(Integer, primary_key=True)
    sample_name = Column(UnicodeText)
    report_id = Column(
        Integer,
        ForeignKey("report.report_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    report = relationship("Report", back_populates="samples")
    data = relationship("SampleData", back_populates="sample", passive_deletes="all")
