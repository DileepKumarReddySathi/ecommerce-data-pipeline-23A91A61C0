import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os

@pytest.fixture(scope="session")
def db_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME"),
    )
    engine = create_engine(url)
    yield engine
    engine.dispose()
import subprocess
import pytest

@pytest.fixture(scope="session", autouse=True)
def run_pipeline_once():
    subprocess.run(
        ["python", "scripts/pipeline_orchestrator.py"],
        check=True
    )
