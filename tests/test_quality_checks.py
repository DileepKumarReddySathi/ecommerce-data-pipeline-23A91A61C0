import json
import os
from scripts.data_generation.generate_data import generate_all_data



REPORT = "data/processed/pipeline_execution_report.json"

def test_quality_report_exists():
    assert os.path.exists(REPORT)

def test_quality_score_present():
    with open(REPORT) as f:
        data = json.load(f)
    assert "data_quality_summary" in data
    assert data["data_quality_summary"]["quality_score"] >= 0
