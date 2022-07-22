from pathlib import Path

import structlog
from owid.catalog import DatasetMeta
from owid.catalog.catalogs import CatalogSeries
from sqlalchemy import JSON, Boolean, Column, Integer, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from utils import sanitize_table_path

Base = declarative_base()


log = structlog.get_logger()


# NOTE: not having type hints is quite limiting, ideally we would make this work with sqlmodel
class MetaDatasetModel(Base):  # type: ignore
    """
    Almost identical copy of DatasetMeta from owid-catalog-py
    """

    __tablename__ = "meta_datasets"

    # TODO: what should we use as the primary key? either we use autoincremented ids or we
    # use paths (e.g. `garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp` is an address to table)
    # what are the pros and cons of each?

    channel = Column(String)
    namespace = Column(String)
    short_name = Column(String, primary_key=True)
    title = Column(String)
    description = Column(String)
    sources = Column(JSON)
    licenses = Column(JSON)
    is_public = Column(Boolean)
    source_checksum = Column(String)
    version = Column(String)

    # this is an attribute of additional_info['grapher_meta']
    grapher_meta = Column(JSON)

    @classmethod
    def from_DatasetMeta(cls, ds: DatasetMeta, channel: str) -> "MetaDatasetModel":
        return MetaDatasetModel(
            channel=channel,
            short_name=ds.short_name,
            namespace=ds.namespace,
            title=ds.title,
            description=ds.description,
            sources=[source.to_dict() for source in ds.sources],
            licenses=[license.to_dict() for license in ds.licenses],
            is_public=ds.is_public,
            source_checksum=ds.source_checksum,
            grapher_meta=ds.additional_info["grapher_meta"]
            if ds.additional_info
            else None,
            version=ds.version,
        )


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
    dimensions = Column(JSON)
    path = Column(String)
    format = Column(String)
    is_public = Column(Boolean)

    def __init__(self, *args, **kwargs):
        # TODO: path could be very long, but how do we guarantee uniqueness of table name
        #   across datasets? or should we just go with table name and use full path only
        #   for non-unique table names?
        kwargs["table_db_name"] = sanitize_table_path(kwargs["path"])
        super().__init__(*args, **kwargs)

    @classmethod
    def from_CatalogSeries(cls, catalog_row: CatalogSeries) -> "MetaTableModel":
        d = catalog_row.to_dict()

        d["dimensions"] = list(d["dimensions"])

        # rename to adhere to DuckDB schema
        d["table_name"] = d.pop("table")
        d["dataset_name"] = d.pop("dataset")

        t = cls(**d)

        is_backport = t.channel == "backport"

        if is_backport:
            missing_dims = {"year", "entity_name", "entity_code", "entity_id"} - set(
                t.dimensions
            )
            assert not missing_dims, f"Missing dimensions: {missing_dims}"

        return t


class MetaVariableModel(Base):  # type: ignore
    __tablename__ = "meta_variables"

    # columns from VariableMeta
    title = Column(String)
    description = Column(String)
    licenses = Column(JSON)
    sources = Column(JSON)
    unit = Column(String)
    short_unit = Column(String)
    display = Column(JSON)

    # this is an attribute of additional_info['grapher_meta']
    grapher_meta = Column(JSON)

    variable_path = Column(String, primary_key=True)
    variable_id = Column(Integer)

    # inferred columns by crawler
    short_name = Column(String)
    table_path = Column(String)
    table_db_name = Column(String)
    dataset_short_name = Column(String)
    variable_type = Column(String)

    # distinct values of years and entities encoded as JSON
    dimension_values = Column(JSON)

    def __init__(self, *args, **kwargs):
        kwargs["variable_path"] = f"{kwargs['table_path']}/{kwargs['short_name']}"
        super().__init__(*args, **kwargs)


def db_init(path: Path) -> Engine:
    eng = create_engine(f"duckdb:///{path}")
    Base.metadata.create_all(eng)
    return eng
