import threading

from fastapi import APIRouter

from app import utils


router = APIRouter()


@router.get(
    "/datasets",
)
def list_all_datasets():
    con = utils.get_readonly_connection(threading.get_ident())
    sql = """
    select title from meta_datasets
    """
    df = con.execute(sql).fetch_df()
    return {"datasets": list(df.title)}


@router.get(
    "/dataset/data",
)
def list_channels():
    """List all available channels."""

    con = utils.get_readonly_connection(threading.get_ident())
    sql = """
    select distinct channel from meta_tables
    """
    df = con.execute(sql).fetch_df()
    return {"channels": list(df.channel)}


@router.get(
    "/dataset/data/{channel}",
)
def list_namespaces(channel: str):
    """List all available namespaces."""

    con = utils.get_readonly_connection(threading.get_ident())
    sql = """
    select distinct namespace from meta_tables
    where channel = (?)
    """
    df = con.execute(sql, parameters=[channel]).fetch_df()
    return {"namespaces": list(df.namespace)}


@router.get(
    "/dataset/data/{channel}/{namespace}",
)
def list_versions(channel: str, namespace: str):
    """List all available versions."""

    con = utils.get_readonly_connection(threading.get_ident())
    sql = """
    select distinct version from meta_tables
    where channel = (?) and namespace = (?)
    """
    df = con.execute(sql, parameters=[channel, namespace]).fetch_df()
    return {"versions": list(df.version)}


@router.get(
    "/dataset/data/{channel}/{namespace}/{version}",
)
def list_datasets(channel: str, namespace: str, version: str):
    """List all available datasets."""

    con = utils.get_readonly_connection(threading.get_ident())
    sql = """
    select distinct dataset_name from meta_tables
    where channel = (?) and namespace = (?) and version = (?)
    """
    df = con.execute(sql, parameters=[channel, namespace, version]).fetch_df()
    return {"datasets": list(df.dataset_name)}


@router.get(
    "/dataset/data/{channel}/{namespace}/{version}/{dataset}",
)
def list_tables(channel: str, namespace: str, version: str, dataset: str):
    """List all available tables."""

    con = utils.get_readonly_connection(threading.get_ident())
    sql = """
    select distinct table_name from meta_tables
    where channel = (?) and namespace = (?) and version = (?) and dataset_name = (?)
    """
    df = con.execute(sql, parameters=[channel, namespace, version, dataset]).fetch_df()
    return {"tables": list(df.table_name)}
