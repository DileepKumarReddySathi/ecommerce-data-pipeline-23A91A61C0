import pandas as pd

def test_fact_dimension_relationships(db_engine):
    with db_engine.connect() as conn:
        df = pd.read_sql("""
            SELECT COUNT(*) cnt
            FROM production.transaction_items ti
            LEFT JOIN production.transactions t
            ON ti.transaction_id = t.transaction_id
            WHERE t.transaction_id IS NULL
        """, conn)
    assert df.iloc[0]["cnt"] == 0

def test_aggregate_consistency(db_engine):
    with db_engine.connect() as conn:
        fact = pd.read_sql(
            "SELECT COALESCE(SUM(line_total), 0) AS total FROM production.transaction_items",
            conn
        )

    assert fact.iloc[0]["total"] >= 0

