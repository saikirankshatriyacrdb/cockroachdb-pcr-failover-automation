import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # CockroachDB Cloud API Configuration
    API_BASE_URL = "https://cockroachlabs.cloud/api/v1"
    API_SECRET_KEY = os.getenv("COCKROACH_API_SECRET_KEY")

    # Cluster Configuration
    PRIMARY_CLUSTER_ID = os.getenv("PRIMARY_CLUSTER_ID")
    STANDBY_CLUSTER_ID = os.getenv("STANDBY_CLUSTER_ID")
    PCR_STREAM_ID = os.getenv("PCR_STREAM_ID")  # Optional, can be discovered

    # Health Check Configuration
    HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", "30"))  # seconds
    PRIMARY_CLUSTER_ENDPOINT = os.getenv("PRIMARY_CLUSTER_ENDPOINT")  # Connection string
    HEALTH_CHECK_TIMEOUT = int(os.getenv("HEALTH_CHECK_TIMEOUT", "10"))  # seconds

    # Failure Detection
    FAILURE_THRESHOLD = int(os.getenv("FAILURE_THRESHOLD", "3"))  # consecutive failures
    FAILOVER_DELAY = int(os.getenv("FAILOVER_DELAY", "60"))  # seconds before failover

    # Failover Configuration
    FAILOVER_TO_LATEST = os.getenv("FAILOVER_TO_LATEST", "true").lower() == "true"
    FAILOVER_AT_TIMESTAMP = os.getenv("FAILOVER_AT_TIMESTAMP")  # Optional ISO timestamp

    # Monitoring
    ENABLE_PROMETHEUS = os.getenv("ENABLE_PROMETHEUS", "false").lower() == "true"
    PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))
