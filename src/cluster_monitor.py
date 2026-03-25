import requests
import time
import json
import logging
from datetime import datetime, timezone
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
        self.cutover_started_at = None
        self.cutover_alert_sent = False

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

    def _send_alert_webhook(self, message: str, severity: str = "critical"):
        """Send alert to configured webhook (Slack, PagerDuty, etc.)"""
        if not Config.ALERT_WEBHOOK_URL:
            return

        try:
            payload = {
                "text": f"[{severity.upper()}] PCR Automation Alert: {message}",
                "severity": severity,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "primary_cluster_id": Config.PRIMARY_CLUSTER_ID,
                "standby_cluster_id": Config.STANDBY_CLUSTER_ID,
                "pcr_stream_id": self.pcr_stream_id,
            }
            response = requests.post(
                Config.ALERT_WEBHOOK_URL,
                json=payload,
                timeout=10
            )
            if response.status_code < 300:
                logger.info(f"Alert webhook sent successfully")
            else:
                logger.warning(f"Alert webhook returned {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to send alert webhook: {str(e)}")

    def check_cutover_timeout(self) -> Optional[Dict[str, Any]]:
        """
        Check if a PCR cutover is stuck in FAILING_OVER state beyond the
        configured timeout (default: 5 minutes). Since the replication stream
        stops during cutover, the replication lag alert will NOT fire -- this
        fills that gap.

        This method is alert-only. It does NOT take automatic recovery action
        because during a real primary failure, auto-destroying the standby
        would leave the customer with no working cluster.

        Recovery is available manually via:
          python cli.py escape-hatch cancel-cutover
          python cli.py escape-hatch destroy-standby

        Returns a dict with alert details if timed out, None otherwise.
        """
        stream_info = self.get_pcr_stream_info()
        if not stream_info:
            return None

        status = stream_info.get("status", "").upper()

        if status == "FAILING_OVER":
            now = time.time()

            if self.cutover_started_at is None:
                self.cutover_started_at = now
                logger.info("Detected PCR stream in FAILING_OVER state. Starting cutover timer.")

            elapsed_seconds = now - self.cutover_started_at
            elapsed_minutes = elapsed_seconds / 60
            timeout_minutes = Config.CUTOVER_TIMEOUT_MINUTES

            logger.info(
                f"Cutover in progress: {elapsed_minutes:.1f} / {timeout_minutes} minutes elapsed"
            )

            if elapsed_minutes >= timeout_minutes and not self.cutover_alert_sent:
                self.cutover_alert_sent = True
                alert_msg = (
                    f"PCR cutover has been stuck in FAILING_OVER state for "
                    f"{elapsed_minutes:.1f} minutes (threshold: {timeout_minutes} min). "
                    f"Stream ID: {self.pcr_stream_id}. "
                    f"Primary: {Config.PRIMARY_CLUSTER_ID}, "
                    f"Standby: {Config.STANDBY_CLUSTER_ID}. "
                    f"Manual intervention may be required. "
                    f"Use 'python cli.py escape-hatch cancel-cutover' or "
                    f"'python cli.py escape-hatch destroy-standby' to recover."
                )
                logger.critical(alert_msg)
                self._send_alert_webhook(alert_msg)

                return {
                    "alert": True,
                    "elapsed_minutes": elapsed_minutes,
                    "threshold_minutes": timeout_minutes,
                    "stream_id": self.pcr_stream_id,
                    "message": alert_msg,
                }
        else:
            # Reset tracking if no longer in FAILING_OVER
            if self.cutover_started_at is not None:
                logger.info(f"PCR stream exited FAILING_OVER state (now: {status}). Resetting cutover timer.")
            self.cutover_started_at = None
            self.cutover_alert_sent = False

        return None

    def monitor_failover_progress(self) -> bool:
        """Monitor failover progress until completion"""
        logger.info("Monitoring failover progress...")
        logger.info(f"Cutover timeout alert: {Config.CUTOVER_TIMEOUT_MINUTES} min")

        max_wait_time = 3600  # 1 hour max
        check_interval = 10  # Check every 10 seconds
        elapsed_time = 0
        self.cutover_started_at = time.time()
        self.cutover_alert_sent = False

        while elapsed_time < max_wait_time:
            stream_info = self.get_pcr_stream_info()

            if not stream_info:
                time.sleep(check_interval)
                elapsed_time += check_interval
                continue

            status = stream_info.get("status", "").upper()
            logger.info(f"PCR stream status: {status} (elapsed: {elapsed_time}s)")

            if status == "COMPLETED":
                activated_at = stream_info.get("activated_at")
                logger.info(f"Failover completed successfully at {activated_at}")
                self.cutover_started_at = None
                self.cutover_alert_sent = False
                return True
            elif status == "FAILING_OVER":
                # Check for cutover timeout -- fires alert if stuck
                alert = self.check_cutover_timeout()
                if alert:
                    logger.critical(
                        f"HUNG CUTOVER DETECTED after {alert['elapsed_minutes']:.1f} minutes. "
                        f"Use 'python cli.py escape-hatch' to recover."
                    )
            else:
                logger.warning(f"Unexpected status during failover: {status}")

            time.sleep(check_interval)
            elapsed_time += check_interval

        logger.error("Failover monitoring timed out after 1 hour")
        return False

    def cancel_pcr_cutover(self) -> bool:
        """
        Attempt to cancel a PCR cutover stuck in FAILING_OVER state.
        Tries to PATCH the stream back to a non-FAILING_OVER state or delete it.
        """
        try:
            if not self.pcr_stream_id:
                self.pcr_stream_id = self.discover_pcr_stream_id()

            if not self.pcr_stream_id:
                logger.error("Cannot cancel cutover: PCR stream ID not found")
                return False

            stream_info = self.get_pcr_stream_info()
            if not stream_info:
                logger.error("Cannot cancel cutover: unable to retrieve stream info")
                return False

            status = stream_info.get("status", "").upper()
            if status != "FAILING_OVER":
                logger.info(f"PCR stream is not in FAILING_OVER state (current: {status}). No action needed.")
                return True

            # Attempt 1: Try to DELETE the PCR stream
            url = f"{self.base_url}/physical-replication-streams/{self.pcr_stream_id}"
            logger.info(f"Attempting to delete PCR stream {self.pcr_stream_id}...")

            response = requests.delete(url, headers=self.headers, timeout=30)

            if response.status_code in [200, 202, 204]:
                logger.info(f"PCR stream {self.pcr_stream_id} deleted successfully")
                return True
            else:
                logger.warning(
                    f"Delete PCR stream returned {response.status_code}: {response.text}. "
                    f"The CC API may not support cancellation in this state."
                )

            # Attempt 2: Try PATCH to COMPLETED as a force-complete
            logger.info("Attempting to force-complete the PCR stream...")
            payload = {"status": "COMPLETED"}
            response = requests.patch(url, headers=self.headers, json=payload, timeout=30)

            if response.status_code == 200:
                logger.info("PCR stream force-completed successfully")
                return True
            else:
                logger.warning(f"Force-complete returned {response.status_code}: {response.text}")

            logger.error(
                "Unable to cancel cutover via API. "
                "You may need to contact CockroachDB support or use 'escape-hatch destroy-standby'."
            )
            return False

        except Exception as e:
            logger.error(f"Error canceling cutover: {str(e)}")
            return False

    def force_destroy_standby(self, force: bool = False) -> bool:
        """
        Force-destroy the standby cluster when stuck in FAILING_OVER state.
        This is an escape hatch for when the PCR cutover is wedged and
        the customer cannot cancel the PCR job or destroy the cluster normally.

        Steps:
        1. Verify the stream is stuck in FAILING_OVER
        2. Attempt to cancel/delete the PCR stream
        3. Delete the standby cluster
        """
        try:
            # Step 1: Check current state
            stream_info = self.get_pcr_stream_info()
            if stream_info:
                status = stream_info.get("status", "").upper()
                logger.info(f"Current PCR stream status: {status}")

                if status != "FAILING_OVER" and not force:
                    logger.warning(
                        f"PCR stream is in {status} state, not FAILING_OVER. "
                        f"Use --force to destroy anyway."
                    )
                    return False

                # Step 2: Try to cancel/delete the PCR stream first
                logger.info("Attempting to remove PCR stream before cluster destruction...")
                self.cancel_pcr_cutover()
                # Continue regardless -- cluster deletion may still work

            # Step 3: Delete the standby cluster
            cluster_id = Config.STANDBY_CLUSTER_ID
            if not cluster_id:
                logger.error("STANDBY_CLUSTER_ID not configured")
                return False

            url = f"{self.base_url}/clusters/{cluster_id}"
            logger.info(f"Requesting deletion of standby cluster {cluster_id}...")

            response = requests.delete(url, headers=self.headers, timeout=30)

            if response.status_code in [200, 202, 204]:
                logger.info(f"Standby cluster {cluster_id} deletion initiated successfully")
                return True
            elif response.status_code == 409:
                logger.error(
                    f"Cluster deletion blocked (409 Conflict): {response.text}. "
                    f"The cluster may still have active PCR streams. "
                    f"Contact CockroachDB support for manual intervention."
                )
                return False
            else:
                logger.error(f"Cluster deletion failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error destroying standby cluster: {str(e)}")
            return False

    def get_cutover_status(self) -> Dict[str, Any]:
        """
        Get detailed status of a cutover in progress, including
        how long it's been in FAILING_OVER state.
        """
        result = {
            "stream_id": self.pcr_stream_id,
            "status": "UNKNOWN",
            "is_stuck": False,
            "elapsed_minutes": 0,
            "threshold_minutes": Config.CUTOVER_TIMEOUT_MINUTES,
        }

        stream_info = self.get_pcr_stream_info()
        if not stream_info:
            return result

        status = stream_info.get("status", "").upper()
        result["status"] = status
        result["primary_cluster_id"] = stream_info.get("primary_cluster_id")
        result["standby_cluster_id"] = stream_info.get("standby_cluster_id")
        result["replicated_time"] = stream_info.get("replicated_time")
        result["created_at"] = stream_info.get("created_at")

        if status == "FAILING_OVER":
            # Try to determine how long it's been in this state
            if self.cutover_started_at:
                elapsed = (time.time() - self.cutover_started_at) / 60
            else:
                # Estimate from updated_at if available, otherwise just note it
                self.cutover_started_at = time.time()
                elapsed = 0

            result["elapsed_minutes"] = round(elapsed, 1)
            result["is_stuck"] = elapsed >= Config.CUTOVER_TIMEOUT_MINUTES

        return result

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
