from pathlib import Path

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


def test_metadata_for_variable():
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
        "display": {
            "name": None,
            "unit": None,
            "shortUnit": None,
            "includeInTable": None,
        },
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
                "values": {
                    "1988": None,
                    "1989": None,
                    "1990": None,
                    "1991": None,
                    "1992": None,
                    "1993": None,
                    "1994": None,
                    "1995": None,
                    "1996": None,
                    "1997": None,
                    "1998": None,
                    "1999": None,
                    "2000": None,
                    "2001": None,
                    "2002": None,
                    "2003": None,
                },
            },
            "entities": {
                "type": "int",
                "values": {"13": {"name": "United States", "code": "USA"}},
            },
        },
    }
