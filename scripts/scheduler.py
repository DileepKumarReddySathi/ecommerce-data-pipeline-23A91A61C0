import schedule
import subprocess
import time
import yaml
import os
import logging
from datetime import datetime
from pathlib import Path

LOCK_FILE = "/tmp/pipeline_scheduler.lock"

# -----------------------------
# Logging
# -----------------------------
LOG_FILE = "logs/scheduler_activity.log"
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -----------------------------
# Load Config
# -----------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

RUN_TIME = config["scheduler"]["run_time"]
PIPELINE_SCRIPT = config["pipeline"]["orchestrator_path"]

# -----------------------------
# Concurrency Protection
# -----------------------------
def acquire_lock():
    if os.path.exists(LOCK_FILE):
        logging.warning("Previous pipeline still running. Skipping execution.")
        return False
    Path(LOCK_FILE).touch()
    return True


def release_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

# -----------------------------
# Pipeline Execution
# -----------------------------
def run_pipeline():
    if not acquire_lock():
        return

    start = datetime.utcnow()
    logging.info("🚀 Scheduled pipeline execution started")

    try:
        result = subprocess.run(
            ["python", PIPELINE_SCRIPT],
            capture_output=True,
            text=True,
            check=True
        )

        logging.info("✅ Pipeline completed successfully")
        logging.info(result.stdout)

        # Run cleanup after success
        subprocess.run(["python", "scripts/cleanup_old_data.py"], check=True)

    except subprocess.CalledProcessError as e:
        logging.error("❌ Pipeline execution failed")
        logging.error(e.stderr)

    finally:
        release_lock()
        duration = (datetime.utcnow() - start).total_seconds()
        logging.info(f"⏱ Execution finished in {duration:.2f} seconds")

# -----------------------------
# Scheduler Setup
# -----------------------------
schedule.every().day.at(RUN_TIME).do(run_pipeline)

logging.info("🕒 Scheduler started")
logging.info(f"Daily execution scheduled at {RUN_TIME} UTC")

# -----------------------------
# Scheduler Loop
# -----------------------------
while True:
    try:
        schedule.run_pending()
        time.sleep(30)
    except Exception as e:
        logging.error(f"Scheduler error: {str(e)}")
        time.sleep(60)
