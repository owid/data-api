from app.v1 import metadata

# def test_extract_dimension_values():
#     df = pd.DataFrame(
#         {
#             "entity_id": [1, 2, 3],
#             "entity_name": ["A", "B", "C"],
#             "entity_code": ["c1", "c2", "c3"],
#             "year": [2000, 2001, 2002],
#             # "value": [1, 2, 3],
#         }
#     ).set_index(["year", "entity_id", "entity_code", "entity_name"])

#     dim_values = crawl._extract_dimension_values(df.index)
#     assert dim_values == {
#         "entity_zip": ["1|A|c1", "2|B|c2", "3|C|c3"],
#         "year": [2000, 2001, 2002],
#     }


def test_parse_dimension_values():
    dim_values = {
        "entity_zip": ["1|A|c1", "2|B|c2", "3|C|c3"],
        "year": [2000, 2001, 2002],
    }
    dims = metadata._parse_dimension_values(dim_values)
    assert dims == {
        "years": metadata.Dimension(
            type="int",
            values=[
                metadata.DimensionProperties(id=2000, name=None, code=None),
                metadata.DimensionProperties(id=2001, name=None, code=None),
                metadata.DimensionProperties(id=2002, name=None, code=None),
            ],
        ),
        "entities": metadata.Dimension(
            type="int",
            values=[
                metadata.DimensionProperties(id=1, name="A", code="c1"),
                metadata.DimensionProperties(id=2, name="B", code="c2"),
                metadata.DimensionProperties(id=3, name="C", code="c3"),
            ],
        ),
    }
