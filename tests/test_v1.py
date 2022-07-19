import pandas as pd
from pathlib import Path
import io

from fastapi.testclient import TestClient

from app.main import app, settings

client = TestClient(app)

# mock settings
settings.DUCKDB_PATH = Path("tests/sample_duck.db")


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_data_for_variable():
    response = client.get("/v1/variableById/data/42539")
    assert response.status_code == 200
    assert set(response.json().keys()) == {
        "year",
        "entity_name",
        "entity_id",
        "entity_code",
        "value",
    }


def test_metadata_for_backported_variable():
    # this test requires connection to the database, this is only temporary and will change once we start getting
    # metadata from the catalog instead of the database
    response = client.get("/v1/variableById/metadata/42539")
    assert response.status_code == 200
    assert response.json() == {
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
            "additionalInfo": "Roads - Historical Statistics of the United States, Colonial Times to 1970, Volume 1 and 2. Bureau of the Census, Washington D.C. see Chapter Q - Transportation, Q50-63. Link: https://www2.census.gov/library/publications/1975/compendia/hist_stats_colonial-1970/hist_stats_colonial-1970p2-chQ.pdf;\nDiesel locomotives - Historical Statistics of the United States, Colonial Times to 1970, Volume 1 and 2. Bureau of the Census, Washington D.C. see Chapter Q - Transportation, Series Q284-312: Railroad mileage, equipment, and passenger traffic and revenue: 1890 to 1970. Link: https://www2.census.gov/library/publications/1975/compendia/hist_stats_colonial-1970/hist_stats_colonial-1970p2-chQ.pdf;\nAgricultural tractor, ATM, Aviation passenger-km, Credit and debit payments, Card payments, MRI units, Newspapers, Retail locations accepting card, Rail passenger-km, Steamships (tons), Crude steel production (blast oxygen furnaces)/(electric furnaces), Synthetic (non-cellulosic) fibres, Commercial vehicles - Comin and Hobijn (2004). Link: http://www.nber.org/data/chat/;\nMail and telegrams - Mitchell (1998) International Historical Statistics: the Americas, 1970-2000, 5th Ed",
        },
        "type": "FLOAT",
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


TEST_RESPONSE_JSON = {
    "country": ["Afghanistan", "Afghanistan"],
    "population": [3280000.0, 4207000.0],
    "year": [1820, 1870],
}


def test_data_for_etl_table_json_format():
    # this test requires connection to the database, this is only temporary and will change once we start getting
    # metadata from the catalog instead of the database
    response = client.get(
        "/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp.json",
        params={"limit": 2, "columns": "year,country,population"},
    )
    assert response.status_code == 200
    assert response.json() == TEST_RESPONSE_JSON


def test_data_for_etl_table_csv_format():
    response = client.get(
        "/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp.csv",
        params={"limit": 2, "columns": "year,country,population"},
    )
    assert response.status_code == 200
    df = pd.read_csv(io.StringIO(response.text))
    assert df.to_dict(orient="list") == TEST_RESPONSE_JSON


def test_data_for_etl_table_feather_format():
    response = client.get(
        "/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp.feather",
        params={"limit": 2, "columns": "year,country,population"},
    )
    assert response.status_code == 200
    df = pd.read_feather(io.BytesIO(response.content))
    assert df.to_dict(orient="list") == TEST_RESPONSE_JSON


def test_metadata_for_etl_table():
    response = client.get(
        "/v1/dataset/metadata/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
        params={"limit": 2},
    )
    assert response.status_code == 200
    js = response.json()

    # trim long fields
    js["dataset"]["description"] = js["dataset"]["description"][:20]

    assert js == {
        "variables": [
            {
                "title": "GDP per capita",
                "description": None,
                "licenses": [],
                "sources": [],
                "unit": "2011 int-$",
                "short_unit": "$",
                "short_name": "gdp_per_capita",
                "table_path": "garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
                "table_db_name": "garden__ggdc__2020_10_01__ggdc_maddison__maddison_gdp",
                "dataset_short_name": "ggdc_maddison",
                "variable_type": "FLOAT",
            },
            {
                "title": "Population",
                "description": None,
                "licenses": [],
                "sources": [],
                "unit": "people",
                "short_unit": None,
                "short_name": "population",
                "table_path": "garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
                "table_db_name": "garden__ggdc__2020_10_01__ggdc_maddison__maddison_gdp",
                "dataset_short_name": "ggdc_maddison",
                "variable_type": "FLOAT",
            },
            {
                "title": "GDP",
                "description": "Gross domestic product measured in international-$ using 2011 prices to adjust for price changes over time (inflation) and price differences between countries. Calculated by multiplying GDP per capita with population.",
                "licenses": [],
                "sources": [],
                "unit": "2011 int-$",
                "short_unit": "$",
                "short_name": "gdp",
                "table_path": "garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
                "table_db_name": "garden__ggdc__2020_10_01__ggdc_maddison__maddison_gdp",
                "dataset_short_name": "ggdc_maddison",
                "variable_type": "FLOAT",
            },
        ],
        "table": {
            "table_name": "maddison_gdp",
            "dataset_name": "ggdc_maddison",
            "table_db_name": "garden__ggdc__2020_10_01__ggdc_maddison__maddison_gdp",
            "version": "2020-10-01",
            "namespace": "ggdc",
            "channel": "garden",
            "checksum": "7236fb37ff655adc0d9924a9e79937ed",
            "dimensions": ["country", "year"],
            "path": "garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
            "format": "feather",
            "is_public": True,
        },
        "dataset": {
            "channel": "garden",
            "namespace": "ggdc",
            "short_name": "ggdc_maddison",
            "title": "Maddison Project Database (GGDC, 2020)",
            "description": "Notes:\n- Tanzania re",
            "sources": [
                {
                    "name": "Maddison Project Database 2020 (Bolt and van Zanden, 2020)",
                    "url": "https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2020",
                    "source_data_url": "https://www.rug.nl/ggdc/historicaldevelopment/maddison/data/mpd2020.xlsx",
                    "owid_data_url": "https://walden.nyc3.digitaloceanspaces.com/ggdc/2020-10-01/ggdc_maddison.xlsx",
                    "date_accessed": "2022-04-12",
                    "publication_date": "2020-10-01",
                    "publication_year": 2020,
                    "published_by": "Bolt, Jutta and Jan Luiten van Zanden (2020), “Maddison style estimates of the evolution of the world economy. A new 2020 update“.",
                    "publisher_source": "The Maddison Project Database is based on the work of many researchers that have produced estimates of\neconomic growth for individual countries. The full list of sources for this historical data is given for each country below.\n",
                }
            ],
            "licenses": [
                {
                    "name": "Creative Commons BY 4.0",
                    "url": "https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2020",
                }
            ],
            "is_public": True,
            "source_checksum": "a63167b871d470beb15546b565ed2185",
            "version": "2020-10-01",
        },
    }


def test_search():
    response = client.get(
        "/v1/search",
        params={"term": "population"},
    )
    assert response.status_code == 200
    js = response.json()
    assert js == [
        {
            "data_url": "/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
            "dataset_title": "Maddison Project Database (GGDC, 2020)",
            "match": 1.8276277047674334,
            "metadata_url": "/v1/dataset/metadata/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
            "table_name": "maddison_gdp",
            "variable_description": None,
            "variable_name": "population",
            "variable_title": "Population",
        },
        {
            "data_url": "/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
            "dataset_title": "Maddison Project Database (GGDC, 2020)",
            "match": 1.5464542117262898,
            "metadata_url": "/v1/dataset/metadata/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
            "table_name": "maddison_gdp",
            "variable_description": "Gross domestic product measured in international-$ "
            "using 2011 prices to adjust for price changes over "
            "time (inflation) and price differences between "
            "countries. Calculated by multiplying GDP per capita "
            "with population.",
            "variable_name": "gdp",
            "variable_title": "GDP",
        },
    ]
