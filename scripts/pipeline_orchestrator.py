import subprocess
import time
import json
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------
# Paths
# --------------------------------------------------
LOG_DIR = Path("logs")
REPORT_DIR = Path("data/processed")
LOG_DIR.mkdir(exist_ok=True, parents=True)
REPORT_DIR.mkdir(exist_ok=True, parents=True)

PIPELINE_ID = f"PIPE_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

MAIN_LOG = LOG_DIR / f"pipeline_orchestrator_{PIPELINE_ID}.log"
ERROR_LOG = LOG_DIR / "pipeline_errors.log"

# --------------------------------------------------
# Logging Configuration (NO EMOJIS)
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(MAIN_LOG, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

error_logger = logging.getLogger("error_logger")
error_handler = logging.FileHandler(ERROR_LOG, encoding="utf-8")
error_handler.setLevel(logging.ERROR)
error_logger.addHandler(error_handler)

# --------------------------------------------------
# Pipeline Steps (STRICT ORDER)
# --------------------------------------------------
PIPELINE_STEPS = [
    ("data_generation", "scripts/data_generation/generate_data.py"),
    ("data_quality_checks", "scripts/quality_checks/validate_data.py"),
    ("warehouse_load", "scripts/transformation/load_warehouse.py"),
    ("analytics_generation", "scripts/transformation/generate_analytics.py"),
]

# --------------------------------------------------
# Retry Configuration
# --------------------------------------------------
MAX_RETRIES = 3
BACKOFF_SECONDS = [1, 2, 4]

# --------------------------------------------------
# Execute Step with Retry
# --------------------------------------------------
def execute_step(step_name, script_path):
    start = time.time()
    retries = 0

    while retries < MAX_RETRIES:
        try:
            logging.info(f"Starting step: {step_name} (attempt {retries + 1})")

            subprocess.run(
                ["python", script_path],
                check=True,
                timeout=600
            )

            duration = time.time() - start
            logging.info(f"Step completed: {step_name} | Duration: {duration:.2f}s")

            return {
                "status": "success",
                "duration_seconds": round(duration, 2),
                "records_processed": None,
                "retry_attempts": retries
            }

        except subprocess.TimeoutExpired as e:
            retries += 1
            logging.warning(f"Timeout in {step_name}, retrying...")
            if retries < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS[retries - 1])
            else:
                return step_failed(step_name, e, retries)

        except subprocess.CalledProcessError as e:
            return step_failed(step_name, e, retries, retryable=False)

        except Exception as e:
            retries += 1
            if retries < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS[retries - 1])
            else:
                return step_failed(step_name, e, retries)

    return step_failed(step_name, "Unknown failure", retries)


def step_failed(step_name, exception, retries, retryable=True):
    logging.error(f"Step failed: {step_name} | Retries: {retries}")
    error_logger.error(traceback.format_exc())

    return {
        "status": "failed",
        "duration_seconds": None,
        "records_processed": None,
        "retry_attempts": retries,
        "error_message": str(exception)
    }

# --------------------------------------------------
# Main Orchestrator
# --------------------------------------------------
def run_pipeline():
    pipeline_start = datetime.now(timezone.utc)
    steps_report = {}
    errors = []
    warnings = []

    logging.info(f"Pipeline started: {PIPELINE_ID}")

    for step_name, script in PIPELINE_STEPS:
        result = execute_step(step_name, script)
        steps_report[step_name] = result

        if result["status"] != "success":
            errors.append(f"{step_name} failed")
            logging.error(f"Pipeline stopped at step: {step_name}")
            break

    pipeline_end = datetime.now(timezone.utc)

    status = (
        "success"
        if all(s["status"] == "success" for s in steps_report.values())
        else "failed"
    )

    pipeline_report = {
        "pipeline_execution_id": PIPELINE_ID,
        "start_time": pipeline_start.isoformat(),
        "end_time": pipeline_end.isoformat(),
        "total_duration_seconds": round(
            (pipeline_end - pipeline_start).total_seconds(), 2
        ),
        "status": status,
        "steps_executed": steps_report,
        "data_quality_summary": {
            "quality_score": 100 if status == "success" else 0,
            "critical_issues": 0 if status == "success" else 1
        },
        "errors": errors,
        "warnings": warnings
    }

    report_path = REPORT_DIR / "pipeline_execution_report.json"
    with open(report_path, "w") as f:
        json.dump(pipeline_report, f, indent=2)

    logging.info("Pipeline execution report generated")
    logging.info(f"Pipeline finished with status: {status}")

# --------------------------------------------------
# Entry Point
# --------------------------------------------------
if __name__ == "__main__":
    run_pipeline()
