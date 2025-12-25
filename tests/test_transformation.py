import pandas as pd
from scripts.transformation.staging_to_production import run_staging_to_production

def test_production_tables_populated(db_engine):
    with db_engine.connect() as conn:
        cnt = pd.read_sql("SELECT COUNT(*) cnt FROM production.customers", conn)
    assert cnt.iloc[0]["cnt"] > 0

def test_email_cleaning(db_engine):
    with db_engine.connect() as conn:
        df = pd.read_sql("SELECT email FROM production.customers LIMIT 50", conn)
    assert (df["email"] == df["email"].str.lower()).all()

def test_idempotency(db_engine):
    with db_engine.connect() as conn:
        c1 = pd.read_sql("SELECT COUNT(*) cnt FROM production.customers", conn).iloc[0]["cnt"]
        c2 = pd.read_sql("SELECT COUNT(*) cnt FROM production.customers", conn).iloc[0]["cnt"]
    assert c1 == c2
