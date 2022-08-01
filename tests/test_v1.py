import io
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from app.main import app, settings

client = TestClient(app)

# mock settings
settings.DUCKDB_PATH = Path("tests/sample_duck.db")


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_variableById_data_for_variable():
    response = client.get("/v1/variableById/data/42539")
    assert response.status_code == 200
    assert set(response.json().keys()) == {
        "years",
        "entity_names",
        "entities",
        "entity_codes",
        "values",
    }


def test_variableById_metadata_for_backported_variable():
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
                    {"id": 1800},
                    {"id": 1803},
                    {"id": 1805},
                    {"id": 1807},
                    {"id": 1808},
                    {"id": 1809},
                    {"id": 1810},
                    {"id": 1811},
                    {"id": 1812},
                    {"id": 1813},
                    {"id": 1814},
                    {"id": 1815},
                    {"id": 1816},
                    {"id": 1817},
                    {"id": 1818},
                    {"id": 1819},
                    {"id": 1820},
                    {"id": 1821},
                    {"id": 1822},
                    {"id": 1823},
                    {"id": 1824},
                    {"id": 1825},
                    {"id": 1826},
                    {"id": 1827},
                    {"id": 1828},
                    {"id": 1829},
                    {"id": 1830},
                    {"id": 1831},
                    {"id": 1832},
                    {"id": 1833},
                    {"id": 1834},
                    {"id": 1835},
                    {"id": 1836},
                    {"id": 1837},
                    {"id": 1838},
                    {"id": 1839},
                    {"id": 1840},
                    {"id": 1841},
                    {"id": 1842},
                    {"id": 1843},
                    {"id": 1844},
                    {"id": 1845},
                    {"id": 1846},
                    {"id": 1847},
                    {"id": 1848},
                    {"id": 1849},
                    {"id": 1850},
                    {"id": 1851},
                    {"id": 1852},
                    {"id": 1853},
                    {"id": 1854},
                    {"id": 1855},
                    {"id": 1856},
                    {"id": 1857},
                    {"id": 1858},
                    {"id": 1859},
                    {"id": 1860},
                    {"id": 1861},
                    {"id": 1862},
                    {"id": 1863},
                    {"id": 1864},
                    {"id": 1865},
                    {"id": 1866},
                    {"id": 1867},
                    {"id": 1868},
                    {"id": 1869},
                    {"id": 1870},
                    {"id": 1871},
                    {"id": 1872},
                    {"id": 1873},
                    {"id": 1874},
                    {"id": 1875},
                    {"id": 1876},
                    {"id": 1877},
                    {"id": 1878},
                    {"id": 1879},
                    {"id": 1880},
                    {"id": 1881},
                    {"id": 1882},
                    {"id": 1883},
                    {"id": 1884},
                    {"id": 1885},
                    {"id": 1886},
                    {"id": 1887},
                    {"id": 1888},
                    {"id": 1889},
                    {"id": 1890},
                    {"id": 1891},
                    {"id": 1892},
                    {"id": 1893},
                    {"id": 1894},
                    {"id": 1895},
                    {"id": 1896},
                    {"id": 1897},
                    {"id": 1898},
                    {"id": 1899},
                    {"id": 1900},
                    {"id": 1901},
                    {"id": 1902},
                    {"id": 1903},
                    {"id": 1904},
                    {"id": 1905},
                    {"id": 1906},
                    {"id": 1907},
                    {"id": 1908},
                    {"id": 1909},
                    {"id": 1910},
                    {"id": 1911},
                    {"id": 1912},
                    {"id": 1913},
                    {"id": 1914},
                    {"id": 1915},
                    {"id": 1916},
                    {"id": 1917},
                    {"id": 1918},
                    {"id": 1919},
                    {"id": 1920},
                    {"id": 1921},
                    {"id": 1922},
                    {"id": 1923},
                    {"id": 1924},
                    {"id": 1925},
                    {"id": 1926},
                    {"id": 1927},
                    {"id": 1928},
                    {"id": 1929},
                    {"id": 1930},
                    {"id": 1931},
                    {"id": 1932},
                    {"id": 1933},
                    {"id": 1934},
                    {"id": 1935},
                    {"id": 1936},
                    {"id": 1937},
                    {"id": 1938},
                    {"id": 1939},
                    {"id": 1940},
                    {"id": 1941},
                    {"id": 1942},
                    {"id": 1943},
                    {"id": 1944},
                    {"id": 1945},
                    {"id": 1946},
                    {"id": 1947},
                    {"id": 1948},
                    {"id": 1949},
                    {"id": 1950},
                    {"id": 1951},
                    {"id": 1952},
                    {"id": 1953},
                    {"id": 1954},
                    {"id": 1955},
                    {"id": 1956},
                    {"id": 1957},
                    {"id": 1958},
                    {"id": 1959},
                    {"id": 1960},
                    {"id": 1961},
                    {"id": 1962},
                    {"id": 1963},
                    {"id": 1964},
                    {"id": 1965},
                    {"id": 1966},
                    {"id": 1967},
                    {"id": 1968},
                    {"id": 1969},
                    {"id": 1970},
                    {"id": 1971},
                    {"id": 1972},
                    {"id": 1973},
                    {"id": 1974},
                    {"id": 1975},
                    {"id": 1976},
                    {"id": 1977},
                    {"id": 1978},
                    {"id": 1979},
                    {"id": 1980},
                    {"id": 1981},
                    {"id": 1982},
                    {"id": 1983},
                    {"id": 1984},
                    {"id": 1985},
                    {"id": 1986},
                    {"id": 1987},
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


def test_dataset_data_for_etl_table_json_format():
    # this test requires connection to the database, this is only temporary and will change once we start getting
    # metadata from the catalog instead of the database
    response = client.get(
        "/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp.json",
        params={"limit": 2, "columns": "year,country,population"},
    )
    assert response.status_code == 200
    assert response.json() == TEST_RESPONSE_JSON


def test_dataset_data_for_etl_table_csv_format():
    response = client.get(
        "/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp.csv",
        params={"limit": 2, "columns": "year,country,population"},
    )
    assert response.status_code == 200
    df = pd.read_csv(io.StringIO(response.text))
    assert df.to_dict(orient="list") == TEST_RESPONSE_JSON


def test_dataset_data_for_etl_table_feather_format():
    response = client.get(
        "/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp.feather",
        params={"limit": 2, "columns": "year,country,population"},
    )
    assert response.status_code == 200
    df = pd.read_feather(io.BytesIO(response.content))
    assert df.to_dict(orient="list") == TEST_RESPONSE_JSON


def test_dataset_metadata_for_etl_table():
    response = client.get(
        "/v1/dataset/metadata/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
        params={"limit": 2},
    )
    assert response.status_code == 200
    js = response.json()

    # trim long fields
    js["dataset"]["description"] = js["dataset"]["description"][:20]

    assert js == {
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
            "checksum": "7236fb37ff655adc0d9924a9e79937ed",
            "version": "2020-10-01",
        },
        "table": {
            "table_name": "maddison_gdp",
            "dataset_name": "ggdc_maddison",
            "table_db_name": "garden__ggdc__2020_10_01__ggdc_maddison__maddison_gdp",
            "version": "2020-10-01",
            "namespace": "ggdc",
            "channel": "garden",
            "dimensions": ["country", "year"],
            "path": "garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
            "format": "feather",
            "is_public": True,
        },
        "variables": [
            {
                "title": "GDP per capita",
                "description": None,
                "licenses": [],
                "sources": [],
                "unit": "2011 int-$",
                "short_unit": "$",
                "display": {
                    "entityAnnotationsMap": "Western Offshoots: United States, Canada, Australia and New Zealand",
                    "numDecimalPlaces": 0,
                },
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
                "display": {
                    "entityAnnotationsMap": "Western Offshoots: United States, Canada, Australia and New Zealand"
                },
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
                "display": {
                    "entityAnnotationsMap": "Western Offshoots: United States, Canada, Australia and New Zealand",
                    "numDecimalPlaces": 0,
                },
                "short_name": "gdp",
                "table_path": "garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
                "table_db_name": "garden__ggdc__2020_10_01__ggdc_maddison__maddison_gdp",
                "dataset_short_name": "ggdc_maddison",
                "variable_type": "FLOAT",
            },
        ],
    }


def test_dataset_metadata_for_backported_table():
    response = client.get(
        "/v1/dataset/metadata/backport/owid/latest/dataset_941_technology_adoption__isard__1942__and_others/dataset_941_technology_adoption__isard__1942__and_others",
    )
    assert response.status_code == 200
    response.json()


def test_search():
    response = client.get(
        "/v1/search",
        params={"term": "population"},
    )
    assert response.status_code == 200
    js = response.json()
    assert js == {
        "results": [
            {
                "variable_name": "population",
                "variable_title": "Population",
                "variable_description": "nan",
                "variable_unit": "people",
                "table_name": "maddison_gdp",
                "dataset_title": "Maddison Project Database (GGDC, 2020)",
                "channel": "garden",
                "metadata_url": "/v1/dataset/metadata/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
                "data_url": "/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
                "match": 1.8276277047674334,
            },
            {
                "variable_name": "gdp",
                "variable_title": "GDP",
                "variable_description": "Gross domestic product measured in international-$ using 2011 prices to adjust for price changes over time (inflation) and price differences between countries. Calculated by multiplying GDP per capita with population.",
                "variable_unit": "2011 int-$",
                "table_name": "maddison_gdp",
                "dataset_title": "Maddison Project Database (GGDC, 2020)",
                "channel": "garden",
                "metadata_url": "/v1/dataset/metadata/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
                "data_url": "/v1/dataset/data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp",
                "match": 1.5464542117262898,
            },
        ]
    }
