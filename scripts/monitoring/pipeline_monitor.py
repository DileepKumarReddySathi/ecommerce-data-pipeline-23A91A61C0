import json
import os
import time
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

OUTPUT_PATH = "data/processed"
os.makedirs(OUTPUT_PATH, exist_ok=True)

# -------------------------
# Database connection
# -------------------------
db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME"),
)
engine = create_engine(db_url, future=True)

ALERTS = []

def now_utc():
    return datetime.now(timezone.utc).isoformat()

# -------------------------
# Helper: Add alert
# -------------------------
def add_alert(severity, check, message):
    ALERTS.append({
        "severity": severity,
        "check": check,
        "message": message,
        "timestamp": now_utc()
    })

# -------------------------
# 1. Pipeline Execution Health
# -------------------------
def check_pipeline_execution():
    report_path = f"{OUTPUT_PATH}/pipeline_execution_report.json"
    from datetime import datetime, timezone

    if not os.path.exists(report_path):
        add_alert("critical", "pipeline_execution", "Pipeline has never run")
        return {
            "status": "critical",
            "last_run": None,
            "hours_since_last_run": None,
            "threshold_hours": 25
        }

    with open(report_path) as f:
        report = json.load(f)

    # ✅ FIX: explicitly read timestamp
    last_run_str = report.get("end_time")

    if not last_run_str:
        add_alert("critical", "pipeline_execution", "Missing end_time in report")
        return {
            "status": "critical",
            "last_run": None,
            "hours_since_last_run": None,
            "threshold_hours": 25
        }

    # ✅ FIX: normalize timezone
    last_run = datetime.fromisoformat(last_run_str.replace("Z", ""))
    last_run = last_run.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    hours_since = (now - last_run).total_seconds() / 3600

    status = "ok"
    if hours_since > 25:
        status = "critical"
        add_alert(
            "critical",
            "pipeline_execution",
            f"No pipeline run in {hours_since:.2f} hours"
        )

    return {
        "status": status,
        "last_run": last_run.isoformat(),
        "hours_since_last_run": round(hours_since, 2),
        "threshold_hours": 25
    }


# -------------------------
# 2. Data Freshness
# -------------------------
def check_data_freshness(conn):
    from datetime import datetime, timezone
    from sqlalchemy import text

    freshness_query = text("""
        SELECT
            MAX(t.transaction_date) AS staging_latest,
            MAX(p.created_at) AS production_latest,
            MAX(i.created_at) AS warehouse_latest
        FROM staging.transactions t
        LEFT JOIN production.transactions p ON 1=1
        LEFT JOIN production.transaction_items i ON 1=1
    """)

    row = conn.execute(freshness_query).fetchone()
    now = datetime.now(timezone.utc)

    from datetime import datetime, timezone, date

def lag_hours(ts):
    if ts is None:
        return None

    # ✅ If DATE → convert to DATETIME
    if isinstance(ts, date) and not isinstance(ts, datetime):
        ts = datetime.combine(ts, datetime.min.time())

    # ✅ Make timezone-aware
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    return (datetime.now(timezone.utc) - ts).total_seconds() / 3600


    lags = [
        lag_hours(row.staging_latest),
        lag_hours(row.production_latest),
        lag_hours(row.warehouse_latest)
    ]
    lags = [l for l in lags if l is not None]

    if not lags:
        add_alert("critical", "data_freshness", "No data found in any layer")
        return {
            "status": "critical",
            "staging_latest_record": None,
            "production_latest_record": None,
            "warehouse_latest_record": None,
            "max_lag_hours": None
        }

    max_lag = max(lags)
    status = "ok"

    if max_lag > 24:
        status = "critical"
        add_alert(
            "critical",
            "data_freshness",
            f"Data lag detected: {max_lag:.2f} hours"
        )

    return {
        "status": status,
        "staging_latest_record": str(row.staging_latest),
        "production_latest_record": str(row.production_latest),
        "warehouse_latest_record": str(row.warehouse_latest),
        "max_lag_hours": round(max_lag, 2)
    }


# -------------------------
# 3. Data Volume Anomalies
# -------------------------
def check_volume_anomalies(conn):
    query = """
        SELECT
            transaction_date AS date,
            COUNT(*) AS cnt
        FROM production.transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY transaction_date
        ORDER BY transaction_date
    """

    df = pd.read_sql(query, conn)

    # ✅ HANDLE NO DATA SAFELY
    if df.empty:
        add_alert(
            "warning",
            "data_volume",
            "No transaction data available for volume anomaly detection"
        )
        return {
            "status": "warning",
            "expected_range": None,
            "actual_count": 0,
            "anomaly_detected": False,
            "anomaly_type": None
        }

    mean = df["cnt"].mean()
    std = df["cnt"].std()

    today_cnt = df.iloc[-1]["cnt"]

    upper = mean + (3 * std)
    lower = max(0, mean - (3 * std))

    anomaly = today_cnt > upper or today_cnt < lower
    anomaly_type = None

    if anomaly:
        anomaly_type = "spike" if today_cnt > upper else "drop"
        add_alert(
            "warning",
            "data_volume",
            f"Transaction volume anomaly detected: {today_cnt} (expected {int(lower)}–{int(upper)})"
        )

    return {
        "status": "anomaly_detected" if anomaly else "ok",
        "expected_range": f"{int(lower)}-{int(upper)}",
        "actual_count": int(today_cnt),
        "anomaly_detected": anomaly,
        "anomaly_type": anomaly_type
    }

# -------------------------
# 4. Data Quality Monitoring
# -------------------------
def check_data_quality(conn):
    orphan_query = text("""
        SELECT COUNT(*) FROM production.transaction_items ti
        LEFT JOIN production.transactions t
        ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
    """)

    orphan_count = conn.execute(orphan_query).scalar()

    quality_score = 100 - orphan_count

    status = "ok"
    if quality_score < 95:
        status = "degraded"
        add_alert(
            "warning",
            "data_quality",
            f"Data quality score dropped to {quality_score}"
        )

    return {
        "status": status,
        "quality_score": max(0, quality_score),
        "orphan_records": orphan_count,
        "null_violations": 0
    }

# -------------------------
# 5. Database Health
# -------------------------
def check_database_health():
    start = time.time()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            response_time = (time.time() - start) * 1000

            active_conn = conn.execute(
                text("SELECT COUNT(*) FROM pg_stat_activity")
            ).scalar()

            return {
                "status": "ok",
                "response_time_ms": round(response_time, 2),
                "connections_active": active_conn
            }
    except Exception as e:
        add_alert("critical", "database", str(e))
        return {
            "status": "error",
            "response_time_ms": None,
            "connections_active": None
        }

# -------------------------
# MAIN MONITOR
# -------------------------
def run_monitoring():
    with engine.connect() as conn:
        execution = check_pipeline_execution()
        freshness = check_data_freshness(conn)
        volume = check_volume_anomalies(conn)
        quality = check_data_quality(conn)
        db_health = check_database_health()

    overall_score = 100 - (len(ALERTS) * 10)

    health = "healthy"
    if any(a["severity"] == "critical" for a in ALERTS):
        health = "critical"
    elif any(a["severity"] == "warning" for a in ALERTS):
        health = "degraded"

    report = {
        "monitoring_timestamp": now_utc(),
        "pipeline_health": health,
        "checks": {
            "last_execution": execution,
            "data_freshness": freshness,
            "data_volume_anomalies": volume,
            "data_quality": quality,
            "database_connectivity": db_health
        },
        "alerts": ALERTS,
        "overall_health_score": max(0, overall_score)
    }

    with open(f"{OUTPUT_PATH}/monitoring_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print("✅ Monitoring report generated")

if __name__ == "__main__":
    run_monitoring()
