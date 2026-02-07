import requests
import time
import logging
from typing import Optional, Dict, Any
from src.config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ClusterMonitor:
    """Monitors CockroachDB cluster health and manages failover"""

    def __init__(self):
        self.api_key = Config.API_SECRET_KEY
        self.base_url = Config.API_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "content-type": "application/json"
        }
        self.failure_count = 0
        self.pcr_stream_id = Config.PCR_STREAM_ID

    def check_primary_cluster_health(self) -> bool:
        """
        Check if primary cluster is healthy.
        Returns True if healthy, False otherwise.
        """
        try:
            cluster_url = f"{self.base_url}/clusters/{Config.PRIMARY_CLUSTER_ID}"
            response = requests.get(
                cluster_url,
                headers=self.headers,
                timeout=Config.HEALTH_CHECK_TIMEOUT
            )

            if response.status_code == 200:
                cluster_data = response.json()
                state = cluster_data.get("state", "").upper()
                operation_status = cluster_data.get("operation_status", "").upper()

                healthy_states = ["CREATED"]
                unhealthy_states = ["DELETED", "DELETING"]

                if state in healthy_states and operation_status != "FAILED":
                    logger.info(f"Primary cluster is healthy. State: {state}, Operation: {operation_status}")
                    return True
                elif state in unhealthy_states:
                    logger.warning(f"Primary cluster is in unhealthy state: {state}")
                    return False
                else:
                    logger.warning(f"Primary cluster state: {state}, Operation: {operation_status}")
                    return False
            else:
                logger.error(f"Failed to check cluster status: {response.status_code}")
                return False

        except requests.exceptions.Timeout:
            logger.error("Health check timed out")
            return False
        except requests.exceptions.ConnectionError:
            logger.error("Cannot connect to primary cluster")
            return False
        except Exception as e:
            logger.error(f"Error checking cluster health: {str(e)}")
            return False

    def check_primary_cluster_connectivity(self) -> bool:
        """
        Alternative: Check database connectivity directly.
        This requires a connection string and can execute a simple query.
        """
        try:
            return self.check_primary_cluster_health()
        except Exception as e:
            logger.error(f"Connectivity check failed: {str(e)}")
            return False

    def get_pcr_stream_info(self) -> Optional[Dict[str, Any]]:
        """Get current PCR stream information"""
        try:
            if not self.pcr_stream_id:
                self.pcr_stream_id = self.discover_pcr_stream_id()

            if not self.pcr_stream_id:
                logger.error("PCR stream ID not found")
                return None

            url = f"{self.base_url}/physical-replication-streams/{self.pcr_stream_id}"
            response = requests.get(url, headers=self.headers, timeout=10)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get PCR stream info: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Error getting PCR stream info: {str(e)}")
            return None

    def discover_pcr_stream_id(self) -> Optional[str]:
        """Discover PCR stream ID by checking clusters"""
        try:
            url = f"{self.base_url}/physical-replication-streams"
            params = {"cluster_id": Config.PRIMARY_CLUSTER_ID}
            response = requests.get(url, headers=self.headers, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                streams = data.get("physical_replication_streams", [])

                for stream in streams:
                    if (stream.get("primary_cluster_id") == Config.PRIMARY_CLUSTER_ID and
                        stream.get("standby_cluster_id") == Config.STANDBY_CLUSTER_ID):
                        stream_id = stream.get("id")
                        logger.info(f"Discovered PCR stream ID: {stream_id}")
                        return stream_id

            return None
        except Exception as e:
            logger.error(f"Error discovering PCR stream: {str(e)}")
            return None

    def initiate_failover(self, failover_at: Optional[str] = None) -> bool:
        """
        Initiate failover to standby cluster.

        Args:
            failover_at: Optional ISO timestamp for failover. If None, uses latest consistent time.

        Returns:
            True if failover initiated successfully, False otherwise.
        """
        try:
            if not self.pcr_stream_id:
                self.pcr_stream_id = self.discover_pcr_stream_id()

            if not self.pcr_stream_id:
                logger.error("Cannot initiate failover: PCR stream ID not found")
                return False

            url = f"{self.base_url}/physical-replication-streams/{self.pcr_stream_id}"

            if Config.FAILOVER_TO_LATEST and not failover_at:
                payload = {"status": "FAILING_OVER"}
            else:
                timestamp = failover_at or Config.FAILOVER_AT_TIMESTAMP
                if not timestamp:
                    payload = {"status": "FAILING_OVER"}
                else:
                    payload = {
                        "status": "FAILING_OVER",
                        "failover_at": timestamp
                    }

            logger.info(f"Initiating failover with payload: {payload}")
            response = requests.patch(url, headers=self.headers, json=payload, timeout=30)

            if response.status_code == 200:
                stream_info = response.json()
                logger.info(f"Failover initiated successfully. Stream status: {stream_info.get('status')}")
                return True
            else:
                logger.error(f"Failed to initiate failover: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error initiating failover: {str(e)}")
            return False

    def monitor_failover_progress(self) -> bool:
        """Monitor failover progress until completion"""
        logger.info("Monitoring failover progress...")

        max_wait_time = 3600  # 1 hour max
        check_interval = 10  # Check every 10 seconds
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            stream_info = self.get_pcr_stream_info()

            if not stream_info:
                time.sleep(check_interval)
                elapsed_time += check_interval
                continue

            status = stream_info.get("status", "").upper()
            logger.info(f"PCR stream status: {status}")

            if status == "COMPLETED":
                activated_at = stream_info.get("activated_at")
                logger.info(f"Failover completed successfully at {activated_at}")
                return True
            elif status == "FAILING_OVER":
                logger.info("Failover in progress...")
            else:
                logger.warning(f"Unexpected status during failover: {status}")

            time.sleep(check_interval)
            elapsed_time += check_interval

        logger.error("Failover monitoring timed out")
        return False

    def detect_and_handle_failure(self) -> bool:
        """
        Main failure detection and handling logic.
        Returns True if failover was initiated, False otherwise.
        """
        is_healthy = self.check_primary_cluster_health()

        if is_healthy:
            self.failure_count = 0
            return False

        self.failure_count += 1
        logger.warning(
            f"Primary cluster health check failed. "
            f"Failure count: {self.failure_count}/{Config.FAILURE_THRESHOLD}"
        )

        if self.failure_count >= Config.FAILURE_THRESHOLD:
            logger.critical(
                f"Failure threshold reached ({Config.FAILURE_THRESHOLD}). "
                f"Initiating failover after {Config.FAILOVER_DELAY} second delay..."
            )

            # Wait before failover (allows for transient issues)
            time.sleep(Config.FAILOVER_DELAY)

            # Double-check before failover
            if not self.check_primary_cluster_health():
                logger.critical("Primary cluster still unhealthy. Proceeding with failover.")

                # Check PCR stream status before failover
                stream_info = self.get_pcr_stream_info()
                if stream_info:
                    current_status = stream_info.get("status", "").upper()
                    if current_status not in ["REPLICATING", "STARTING"]:
                        logger.error(
                            f"Cannot failover: PCR stream is in {current_status} status. "
                            f"Expected REPLICATING or STARTING."
                        )
                        return False

                # Initiate failover
                success = self.initiate_failover()

                if success:
                    # Monitor failover progress
                    self.monitor_failover_progress()
                    return True
                else:
                    logger.error("Failed to initiate failover")
                    return False
            else:
                logger.info("Primary cluster recovered. Canceling failover.")
                self.failure_count = 0
                return False

        return False
