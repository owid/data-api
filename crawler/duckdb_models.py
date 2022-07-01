from pathlib import Path

from sqlalchemy import Boolean, Column, Integer, Sequence, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class MetaDatasetModel(Base):  # type: ignore
    """
    Almost identical copy of DatasetMeta from owid-catalog-py
    """

    __tablename__ = "meta_datasets"

    namespace = Column(String)
    short_name = Column(String, primary_key=True)
    title = Column(String)
    description = Column(String)
    sources = Column(String)
    licenses = Column(String)
    is_public = Column(Boolean)
    source_checksum = Column(String)
    version = Column(String)

    # this is an attribute of additional_info['grapher_meta']
    grapher_meta = Column(String)


class MetaTableModel(Base):  # type: ignore
    __tablename__ = "meta_tables"

    # TODO: might be better to use ids as primary key instead of name?
    table_name = Column(String, primary_key=True)
    dataset_name = Column(String)
    table_db_name = Column(String)

    # columns from catalog
    version = Column(String)
    namespace = Column(String)
    channel = Column(String)
    checksum = Column(String)
    dimensions = Column(String)
    path = Column(String)
    format = Column(String)
    is_public = Column(Boolean)

    def __init__(self, *args, **kwargs):
        # TODO: path could be very long, but how do we guarantee uniqueness of table name
        #   across datasets? or should we just go with table name and use full path only
        #   for non-unique table names?
        kwargs["table_db_name"] = kwargs["path"].replace("/", "__")
        super().__init__(*args, **kwargs)


class MetaVariableModel(Base):  # type: ignore
    __tablename__ = "meta_variables"

    # columns from VariableMeta
    title = Column(String)
    description = Column(String)
    licenses = Column(String)
    sources = Column(String)
    unit = Column(String)
    short_unit = Column(String)
    display = Column(String)

    # this is an attribute of additional_info['grapher_meta']
    grapher_meta = Column(String)

    # NOTE: Sequence is needed for duckdb integer primary keys
    variable_id = Column(Integer, Sequence("fakemodel_id_sequence"), primary_key=True)

    # inferred columns by crawler
    short_name = Column(String)
    table_path = Column(String)
    table_db_name = Column(String)
    dataset_short_name = Column(String)
    variable_type = Column(String)

    # distinct values of years and entities encoded as JSON
    years_values = Column(String)
    entities_values = Column(String)


def db_init(path: Path) -> Engine:
    eng = create_engine(f"duckdb:///{path}")
    Base.metadata.create_all(eng)
    return eng
