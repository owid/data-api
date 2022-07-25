from functools import partial
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yaml
from pywebio import config
from pywebio import input as pi
from pywebio import output as po
from pywebio import pin as pn
from pywebio import start_server
from pywebio.pin import pin
from pywebio.session import set_env

from app.v1.schemas import SearchResponse

API_URL = "http://127.0.0.1:8000"

CURRENT_DIR = Path(__file__).parent


def _df_to_array(df: pd.DataFrame) -> list[list[Any]]:
    return [df.columns] + df.to_numpy().tolist()


def _api_search(term, channels) -> pd.DataFrame:
    url = f"{API_URL}/v1/search"
    resp = requests.get(url, params={"term": term, "channels": channels})
    print(f"Searching for {term}...")
    return pd.DataFrame(resp.json()["results"])


def _api_etl_data(data_url, limit: int) -> pd.DataFrame:
    url = f"{API_URL}{data_url}.feather?limit={limit}"
    return pd.read_feather(url)


def _style_truncate(max_width="300px", max_lines=3):
    return f"""
display: -webkit-box;
max-width: {max_width};
-webkit-line-clamp: {max_lines};
-webkit-box-orient: vertical;
overflow: hidden;
"""


def _list_channels() -> list[str]:
    url = f"{API_URL}/v1/dataset/data"
    return requests.get(url).json()["channels"]


def _list_datasets() -> list[str]:
    url = f"{API_URL}/v1/datasets"
    return requests.get(url).json()["datasets"]


def open_popup(choice, result: SearchResponse):
    url = f"{API_URL}{result.metadata_url}"
    resp = requests.get(url)
    assert resp.ok

    js = resp.json()

    if choice == "Variable":
        meta = [v for v in js["variables"] if v["short_name"] == result.variable_name][
            0
        ]

        po.popup(
            "Variable details",
            [po.put_code(yaml.dump(meta), language="yaml")],
            size=po.PopupSize.LARGE,
        )

    elif choice == "Dataset":
        cols = ["title", "description", "unit"]
        df = []
        for v in js["variables"]:
            df.append({c: v[c] for c in cols})

        del js["variables"]

        po.popup(
            "Dataset details",
            [
                po.put_code(yaml.dump(js), language="yaml"),
                po.put_markdown("### Variables"),
                po.put_table(_df_to_array(pd.DataFrame(df))).style("font-size: 14px;"),
            ],
            size=po.PopupSize.LARGE,
        )

    elif choice == "Code":
        (
            _,
            _,
            _,
            _,
            channel,
            namespace,
            version,
            dataset,
            table,
        ) = result.metadata_url.split("/")
        if channel == "backport":
            catalog_snippet = f"""
    table = catalog.find_one(
                table="{table}",
                dataset="{dataset}",
                channels=["backport"],
            )""".strip()
        else:
            catalog_snippet = f"""
    table = catalog.find_one(
                table="{table}",
                namespace="{namespace}",
                dataset="{dataset}",
                channels=["{channel}"],
            )""".strip()

        po.popup(
            "Code snippets",
            [
                po.put_markdown(
                    f"""
            ### Fetch metadata from API
            ```python
            r = requests.get("{API_URL}{result.metadata_url}")
            assert r.ok
            metadata = r.json()
            ```

            ### Fetch data from API
            ```python
            df = pd.read_feather("{API_URL}{result.data_url}.feather")
            df.head()
            ```

            ### Get table from Python API
            ```python
            from owid import catalog
            {catalog_snippet}
            table.head()
            ```
            """
                )
            ],
            size=po.PopupSize.LARGE,
        )


INIT_VALUES = {
    "search_term": "cement",
    "channels": ["garden", "backport"],
}


@config(css_file="static/style.css")
def app():
    channels = _list_channels()
    datasets = _list_datasets()

    set_env(output_animation=False)
    po.put_markdown("""# OWID Data Catalog""")
    pn.put_input("search_term", value=INIT_VALUES["search_term"], label="Search term")
    pn.put_select(
        "channels",
        label="Channels",
        multiple=True,
        options=channels,
        value=INIT_VALUES["channels"],
    )
    pn.put_select(
        "datasets",
        label="Datasets",
        multiple=True,
        options=datasets,
    )

    po.put_markdown("## Results")

    first = True
    while True:
        if first:
            first = False
            # pn.search_term = "cement"
            # pn.channels = ["garden", "backport"]
        else:
            changed = pn.pin_wait_change("search_term", "channels")
            print(changed)

        with po.use_scope("md", clear=True):
            search_term = getattr(pin, "search_term", INIT_VALUES["search_term"])
            channels = getattr(pin, "channels", INIT_VALUES["channels"])
            sf = _api_search(search_term, channels=channels)

            if not sf.empty:

                sf["actions"] = sf.apply(
                    lambda row: po.put_buttons(
                        ["Variable", "Dataset", "Code"],
                        onclick=partial(
                            open_popup, result=SearchResponse(**row.to_dict())
                        ),
                    ).style("min-width: 250px"),
                    axis=1,
                )

                sf["variable_description"] = sf["variable_description"].map(
                    lambda s: po.put_text(s).style(_style_truncate())
                )

                sf["match"] = sf["match"].round(3)

                po.put_table(
                    _df_to_array(
                        sf[
                            [
                                "variable_title",
                                "variable_description",
                                "variable_unit",
                                "dataset_title",
                                "channel",
                                "match",
                                "actions",
                            ]
                        ]
                    )
                ).style("font-size: 14px;")

                # # show top result
                # r: SearchResponse = sf.iloc[0]

                # t = time.time()
                # df = _api_etl_data(r.data_url, limit=20)
                # duration = time.time() - t

                # # limit number of columns
                # df = df.iloc[:, :10]

                # po.put_markdown(
                #     f"""## Table {r.table_name} preview

                # Dataframe shape: {df.shape}
                # Dataframe size: {df.memory_usage().sum() / 1024 / 1024:.2f} MB
                # Latency of pd.read_feather: {duration:.3f} s
                # """
                # )
                # po.put_table(_df_to_array(df))


if __name__ == "__main__":
    start_server(app, port=8001, debug=True, static_dir="demo/static")
