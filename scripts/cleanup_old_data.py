import os
import yaml
import logging
from datetime import datetime, timedelta

# -----------------------------
# Logging
# -----------------------------
LOG_FILE = "logs/scheduler_activity.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -----------------------------
# Load Config
# -----------------------------
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

RETENTION_DAYS = config["retention"]["days"]
TARGET_DIRS = config["retention"]["target_directories"]

CUTOFF_DATE = datetime.now() - timedelta(days=RETENTION_DAYS)

# -----------------------------
# Preservation Rules
# -----------------------------
def should_preserve(filename):
    preserve_keywords = ["summary", "report", "metadata"]
    return any(k in filename.lower() for k in preserve_keywords)

# -----------------------------
# Cleanup Logic
# -----------------------------
def cleanup_directory(directory):
    if not os.path.exists(directory):
        return

    for root, _, files in os.walk(directory):
        for file in files:
            path = os.path.join(root, file)

            try:
                modified_time = datetime.fromtimestamp(os.path.getmtime(path))

                if modified_time < CUTOFF_DATE:
                    if should_preserve(file):
                        continue

                    os.remove(path)
                    logging.info(f"🧹 Removed old file: {path}")

            except Exception as e:
                logging.error(f"Failed to remove {path}: {str(e)}")

# -----------------------------
# Run Cleanup
# -----------------------------
logging.info("🧹 Cleanup process started")

for d in TARGET_DIRS:
    cleanup_directory(d)

logging.info("✅ Cleanup process completed")
