import io
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi.testclient import TestClient

from app.main import app, settings

client = TestClient(app)


def test_variableById_data_for_variable_data_values():
    response = client.get("/v2/variableById/data/42539")
    assert response.status_code == 200
    assert set(response.json().keys()) == {
        "years",
        "entities",
        # "entity_names",
        # "entity_codes",
        "values",
    }


def test_variableById_data_for_variable_catalog():
    response = client.get("/v2/variableById/data/328589")
    assert response.status_code == 200
    assert set(response.json().keys()) == {
        "years",
        "entities",
        # "entity_names",
        # "entity_codes",
        "values",
    }


def test_variableById_metadata_for_variable_data_values():
    response = client.get("/v2/variableById/metadata/42539")
    assert response.status_code == 200
    js = response.json()
    js["source"]["additionalInfo"] = js["source"]["additionalInfo"][:100] + "..."
    assert js == {
        "name": "ATM (Comin and Hobijn (2004))",
        "unit": "",
        "description": "Number of electro-mechanical devices that permit authorized users, typically using machine readable plastic cards, to withdraw cash from their accounts and/or access other services",
        "createdAt": "2017-09-30T19:53:00",
        "updatedAt": "2018-02-28T08:58:52",
        "coverage": "",
        "timespan": "",
        "datasetId": 941,
        "columnOrder": 0,
        "datasetName": "Technology Adoption - Isard (1942) and others",
        "nonRedistributable": False,
        "display": {},
        "source": {
            "id": 6800,
            "name": "Isard (1942) and others",
            "dataPublishedBy": "Isard (1942) and others",
            "dataPublisherSource": "Scholarly work",
            "link": "http://www.jstor.org/stable/1927670",
            "retrievedDate": "28/09/2017",
            "additionalInfo": "Roads - Historical Statistics of the United States, Colonial Times to 1970, Volume 1 and 2. Bureau o...",
        },
        "type": "float",
        "dimensions": {
            "years": {
                "type": "int",
                "values": [
                    {"id": 1988},
                    {"id": 1989},
                    {"id": 1990},
                    {"id": 1991},
                    {"id": 1992},
                    {"id": 1993},
                    {"id": 1994},
                    {"id": 1995},
                    {"id": 1996},
                    {"id": 1997},
                    {"id": 1998},
                    {"id": 1999},
                    {"id": 2000},
                    {"id": 2001},
                    {"id": 2002},
                    {"id": 2003},
                ],
            },
            "entities": {
                "type": "int",
                "values": [{"id": 13, "name": "United States", "code": "USA"}],
            },
        },
    }


def test_variableById_metadata_for_variable_catalog():
    response = client.get("/v2/variableById/metadata/328589")
    assert response.status_code == 200
    js = response.json()
    js["source"]["additionalInfo"] = js["source"]["additionalInfo"][:100] + "..."
    js["dimensions"]["years"]["values"] = js["dimensions"]["years"]["values"][:5]
    js["dimensions"]["entities"]["values"] = js["dimensions"]["entities"]["values"][:5]
    assert js == {
        "name": "GDP",
        "unit": "2011 int-$",
        "shortUnit": "$",
        "description": "Gross domestic product measured in international-$ using 2011 prices to adjust for price changes over time (inflation) and price differences between countries. Calculated by multiplying GDP per capita with population.",
        "createdAt": "2022-08-19T14:17:48",
        "updatedAt": "2022-08-23T11:38:08",
        "coverage": "",
        "timespan": "1-2018",
        "datasetId": 5839,
        "columnOrder": 0,
        "datasetName": "Maddison Project Database (GGDC, 2020)",
        "nonRedistributable": False,
        "display": {
            "unit": "2011 int-$",
            "shortUnit": "$",
            "numDecimalPlaces": 0,
            "entityAnnotationsMap": "Western Offshoots: United States, Canada, Australia and New Zealand",
        },
        "originalMetadata": "{}",
        "source": {
            "id": 21466,
            "name": "Maddison Project Database 2020 (Bolt and van Zanden, 2020)",
            "dataPublishedBy": "Bolt, Jutta and Jan Luiten van Zanden (2020), “Maddison style estimates of the evolution of the world economy. A new 2020 update“.",
            "dataPublisherSource": "The Maddison Project Database is based on the work of many researchers that have produced estimates of\neconomic growth for individual countries. The full list of sources for this historical data is given for each country below.\n",
            "link": "https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2020",
            "retrievedDate": "2022-04-12",
            "additionalInfo": "Notes:\n- Tanzania refers only to Mainland Tanzania.\n- Time series for former countries and territori...",
        },
        "type": "float",
        "dimensions": {
            "years": {
                "type": "int",
                "values": [
                    {"id": 1950},
                    {"id": 1951},
                    {"id": 1952},
                    {"id": 1953},
                    {"id": 1954},
                ],
            },
            "entities": {
                "type": "int",
                "values": [
                    {"id": 15, "name": "Afghanistan", "code": "AFG"},
                    {"id": 16, "name": "Albania", "code": "ALB"},
                    {"id": 17, "name": "Algeria", "code": "DZA"},
                    {"id": 19, "name": "Angola", "code": "AGO"},
                    {"id": 21, "name": "Argentina", "code": "ARG"},
                ],
            },
        },
    }


def test_datasetById_data_for_dataset_data_values():
    response = client.get(
        "/v2/datasetById/data/941.feather",
        params={"columns": ["ATM (Comin and Hobijn (2004))"], "limit": 2},
    )
    assert response.status_code == 200
    df = pd.read_feather(io.BytesIO(response.content))
    assert df.to_dict(orient="list") == {
        "year": [1988, 1989],
        "entityId": [13, 13],
        "entityName": ["United States", "United States"],
        "entityCode": ["USA", "USA"],
        "ATM (Comin and Hobijn (2004))": [80853.95313, 75526.61719],
    }


def test_datasetById_data_for_dataset_catalog():
    response = client.get(
        "/v2/datasetById/data/5839.feather",
        params={"columns": ["GDP", "Population"], "limit": 2},
    )
    assert response.status_code == 200
    df = pd.read_feather(io.BytesIO(response.content))
    df = df.fillna("nan")
    assert df.to_dict(orient="list") == {
        "entityId": [15, 15],
        "year": [1820, 1870],
        "GDP": ["nan", "nan"],
        "Population": [3280000.0, 4207000.0],
    }


def test_datasetById_metadata():
    response = client.get(
        "/v2/datasetById/metadata/5839",
    )
    assert response.status_code == 200
    js = response.json()
    js["sourceDescription"]["additionalInfo"] = js["sourceDescription"][
        "additionalInfo"
    ][:10]
    assert js == {
        "createdAt": "2022-08-17T14:40:10",
        "createdByUserId": 59,
        "dataEditedAt": "2022-08-23T11:38:08",
        "dataEditedByUserId": 59,
        "description": "",
        "id": 5839,
        "isArchived": 0,
        "isPrivate": 0,
        "metadataEditedAt": "2022-08-23T11:38:08",
        "metadataEditedByUserId": 59,
        "name": "Maddison Project Database (GGDC, 2020)",
        "namespace": "ggdc",
        "nonRedistributable": 0,
        "shortName": "ggdc_maddison__2020_10_01",
        "sourceChecksum": "6054c1cd507ea6f82c4dd8d416c77aa6",
        "sourceDescription": {
            "additionalInfo": "Notes:\n- T",
            "dataPublishedBy": "Bolt, Jutta and Jan Luiten van "
            "Zanden (2020), “Maddison style "
            "estimates of the evolution of the "
            "world economy. A new 2020 update“.",
            "dataPublisherSource": "The Maddison Project Database "
            "is based on the work of many "
            "researchers that have produced "
            "estimates of\n"
            "economic growth for individual "
            "countries. The full list of "
            "sources for this historical "
            "data is given for each country "
            "below.\n",
            "link": "https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2020",
            "retrievedDate": "2022-04-12",
        },
        "sourceName": "Maddison Project Database 2020 (Bolt and van Zanden, 2020)",
        "updatedAt": "2022-08-23T11:38:08",
        "variables": [
            {"id": 328589, "name": "GDP", "shortName": "gdp"},
            {"id": 328590, "name": "GDP per capita", "shortName": "gdp_per_capita"},
            {"id": 328591, "name": "Population", "shortName": "population"},
        ],
        "version": "2020-10-01",
    }


def test_variableById_data_for_variable_catalog_with_dimensions():
    response = client.get("/v2/variableById/data/331947")
    assert response.status_code == 200
    assert set(response.json().keys()) == {
        "years",
        "entities",
        # "entity_names",
        # "entity_codes",
        "values",
    }


def test_datasetById_data_for_dataset_catalog_with_dimensions():
    response = client.get(
        "/v2/datasetById/data/5775.feather",
        # params={"columns": ["GDP", "Population"], "limit": 2},
    )
    assert response.status_code == 200
    df = pd.read_feather(io.BytesIO(response.content))
    df = df.fillna("nan")
    assert df.head(2).to_dict(orient="list") == {
        "entityId": [15, 15],
        "year": [-10000, -9000],
        "Population density": [0.023000000044703484, 0.03099999949336052],
    }
