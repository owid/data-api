import time
from typing import Any

import pandas as pd
import requests
from pywebio import start_server
from pywebio.input import FLOAT, TEXT, input
from pywebio.output import (
    put_button,
    put_markdown,
    put_table,
    put_text,
    use_scope,
    put_link,
)
from pywebio.pin import *
from pywebio.session import download, set_env
from dataclasses import dataclass


def _df_to_array(df: pd.DataFrame) -> list[list[Any]]:
    return [df.columns] + df.to_numpy().tolist()


def _api_search(term) -> pd.DataFrame:
    url = "http://127.0.0.1:8000/v1/search"
    resp = requests.get(url, params={"term": term})
    return pd.DataFrame(resp.json())


def _api_etl_data(data_url, limit: int) -> pd.DataFrame:
    url = f"http://127.0.0.1:8000{data_url}"
    resp = requests.get(url, params={"limit": limit})
    return pd.DataFrame(resp.json())


@dataclass
class SearchResult:
    variable_name: str
    variable_title: str
    variable_description: str
    table_name: str
    dataset_title: str
    metadata_url: str
    data_url: str
    match: float


def bmi():
    set_env(output_animation=False)
    put_markdown("""# OWID Data Catalog""")
    put_markdown("""## Search term""")
    put_input("search_term")

    put_markdown("## Results")
    while True:
        search_term = pin_wait_change("search_term")
        with use_scope("md", clear=True):
            # put_markdown(search_term["value"], sanitize=False)
            sf = _api_search(search_term["value"])

            if not sf.empty:

                sf["dataset"] = sf.apply(
                    lambda row: put_link(
                        row.dataset_title, "http://127.0.0.1:8000" + row.metadata_url
                    ),
                    axis=1,
                )

                put_table(
                    _df_to_array(
                        sf[
                            [
                                "variable_title",
                                "variable_description",
                                "dataset",
                                "match",
                            ]
                        ]
                    )
                )

                # show top result
                r: SearchResult = sf.iloc[0]

                t = time.time()
                df = _api_etl_data(r.data_url, limit=20)
                duration = time.time() - t

                # limit number of columns
                df = df.iloc[:, :10]

                put_markdown(
                    f"""## Table {r.table_name} preview

                Dataframe shape: {df.shape}
                Dataframe size: {df.memory_usage().sum() / 1024 / 1024:.2f} MB
                Latency of pd.read_feather: {duration:.3f} s
                """
                )
                put_table(_df_to_array(df))


if __name__ == "__main__":
    start_server(bmi, port=8001, debug=True)
