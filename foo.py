import duckdb

con = duckdb.connect("tests/sample_duck.db", read_only=True)
q = """
select * from meta_variables
"""
df = con.execute(q).fetch_df()
print(len(df))
assert len(df) > 0
