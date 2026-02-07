#!/usr/bin/env python3
"""
Failback Manager - Handles failback operations to restore primary cluster
"""
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


class FailbackManager:
    """Manages failback operations from standby to primary cluster"""

    def __init__(self):
        self.api_key = Config.API_SECRET_KEY
        self.base_url = Config.API_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "content-type": "application/json"
        }
        # For failback, roles are reversed:
        # - Current "standby" (now serving traffic) becomes the new primary
        # - Original "primary" (now recovered) becomes the new standby
        self.failback_primary_id = Config.STANDBY_CLUSTER_ID  # Current active cluster
        self.failback_standby_id = Config.PRIMARY_CLUSTER_ID   # Original primary (recovered)
        self.failback_stream_id = None

    def check_cluster_health(self, cluster_id: str) -> bool:
        """Check if a cluster is healthy and ready"""
        try:
            cluster_url = f"{self.base_url}/clusters/{cluster_id}"
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
                if state in healthy_states and operation_status != "FAILED":
                    logger.info(f"Cluster {cluster_id} is healthy. State: {state}")
                    return True
                else:
                    logger.warning(f"Cluster {cluster_id} state: {state}, Operation: {operation_status}")
                    return False
            else:
                logger.error(f"Failed to check cluster {cluster_id}: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Error checking cluster {cluster_id}: {str(e)}")
            return False

    def create_failback_pcr_stream(self) -> Optional[str]:
        """
        Create a new PCR stream for failback.
        This sets up replication from the current active cluster (standby)
        back to the original primary cluster.

        Returns the PCR stream ID if successful, None otherwise.
        """
        try:
            url = f"{self.base_url}/physical-replication-streams"

            payload = {
                "primary_cluster_id": self.failback_primary_id,  # Current active (was standby)
                "standby_cluster_id": self.failback_standby_id   # Original primary (recovered)
            }

            logger.info(f"Creating failback PCR stream...")
            logger.info(f"  Primary (current active): {self.failback_primary_id}")
            logger.info(f"  Standby (original primary): {self.failback_standby_id}")

            response = requests.post(url, headers=self.headers, json=payload, timeout=30)

            if response.status_code in [200, 201, 202]:
                stream_data = response.json()
                stream_id = stream_data.get("id")
                status = stream_data.get("status", "UNKNOWN")

                logger.info(f"Failback PCR stream created successfully!")
                logger.info(f"  Stream ID: {stream_id}")
                logger.info(f"  Status: {status}")

                self.failback_stream_id = stream_id
                return stream_id
            else:
                logger.error(f"Failed to create failback PCR stream: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return None

        except Exception as e:
            logger.error(f"Error creating failback PCR stream: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def get_failback_stream_info(self) -> Optional[Dict[str, Any]]:
        """Get current failback PCR stream information"""
        try:
            if not self.failback_stream_id:
                self.failback_stream_id = self.discover_failback_stream_id()

            if not self.failback_stream_id:
                logger.warning("Failback PCR stream ID not found")
                return None

            url = f"{self.base_url}/physical-replication-streams/{self.failback_stream_id}"
            response = requests.get(url, headers=self.headers, timeout=10)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get failback stream info: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Error getting failback stream info: {str(e)}")
            return None

    def discover_failback_stream_id(self) -> Optional[str]:
        """Discover failback PCR stream ID"""
        try:
            url = f"{self.base_url}/physical-replication-streams"
            params = {"cluster_id": self.failback_primary_id}
            response = requests.get(url, headers=self.headers, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                streams = data.get("physical_replication_streams", [])

                for stream in streams:
                    if (stream.get("primary_cluster_id") == self.failback_primary_id and
                        stream.get("standby_cluster_id") == self.failback_standby_id):
                        stream_id = stream.get("id")
                        logger.info(f"Discovered failback PCR stream ID: {stream_id}")
                        return stream_id

            return None
        except Exception as e:
            logger.error(f"Error discovering failback stream: {str(e)}")
            return None

    def wait_for_replication_ready(self, max_wait_time: int = 3600) -> bool:
        """
        Wait for the failback PCR stream to be ready (REPLICATING status)

        Args:
            max_wait_time: Maximum time to wait in seconds (default: 1 hour)

        Returns:
            True if stream is ready, False if timeout
        """
        logger.info("Waiting for failback PCR stream to be ready...")

        check_interval = 10  # Check every 10 seconds
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            stream_info = self.get_failback_stream_info()

            if not stream_info:
                time.sleep(check_interval)
                elapsed_time += check_interval
                continue

            status = stream_info.get("status", "").upper()
            logger.info(f"Failback PCR stream status: {status}")

            if status == "REPLICATING":
                replicated_time = stream_info.get("replicated_time", "N/A")
                replication_lag = stream_info.get("replication_lag_seconds", "N/A")
                logger.info(f"Failback PCR stream is REPLICATING!")
                logger.info(f"  Replicated Time: {replicated_time}")
                logger.info(f"  Replication Lag: {replication_lag} seconds")
                return True
            elif status == "COMPLETED":
                logger.warning("Failback PCR stream is already COMPLETED")
                return False
            elif status in ["FAILING_OVER", "STARTING"]:
                logger.info(f"Failback PCR stream is {status}, waiting...")
            else:
                logger.warning(f"Unexpected status: {status}")

            time.sleep(check_interval)
            elapsed_time += check_interval

        logger.error("Timeout waiting for failback PCR stream to be ready")
        return False

    def initiate_failback(self, failover_at: Optional[str] = None) -> bool:
        """
        Initiate failback to the original primary cluster.

        Args:
            failover_at: Optional ISO timestamp for failover. If None, uses latest consistent time.

        Returns:
            True if failback initiated successfully, False otherwise.
        """
        try:
            if not self.failback_stream_id:
                self.failback_stream_id = self.discover_failback_stream_id()

            if not self.failback_stream_id:
                logger.error("Cannot initiate failback: PCR stream ID not found")
                logger.error("Make sure failback PCR stream is created and replicating")
                return False

            url = f"{self.base_url}/physical-replication-streams/{self.failback_stream_id}"

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

            logger.info(f"Initiating failback with payload: {payload}")
            response = requests.patch(url, headers=self.headers, json=payload, timeout=30)

            if response.status_code == 200:
                stream_info = response.json()
                logger.info(f"Failback initiated successfully!")
                logger.info(f"  Stream status: {stream_info.get('status')}")
                return True
            else:
                logger.error(f"Failed to initiate failback: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error initiating failback: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def monitor_failback_progress(self) -> bool:
        """Monitor failback progress until completion"""
        logger.info("Monitoring failback progress...")

        max_wait_time = 3600  # 1 hour max
        check_interval = 10  # Check every 10 seconds
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            stream_info = self.get_failback_stream_info()

            if not stream_info:
                time.sleep(check_interval)
                elapsed_time += check_interval
                continue

            status = stream_info.get("status", "").upper()
            logger.info(f"Failback PCR stream status: {status}")

            if status == "COMPLETED":
                activated_at = stream_info.get("activated_at")
                logger.info(f"Failback completed successfully at {activated_at}")
                return True
            elif status == "FAILING_OVER":
                logger.info("Failback in progress...")
            else:
                logger.warning(f"Unexpected status during failback: {status}")

            time.sleep(check_interval)
            elapsed_time += check_interval

        logger.error("Failback monitoring timed out")
        return False

    def full_failback_process(self, wait_for_replication: bool = True) -> bool:
        """
        Execute the complete failback process:
        1. Verify both clusters are healthy
        2. Create failback PCR stream
        3. Wait for replication to be ready (optional)
        4. Initiate failback
        5. Monitor until completion

        Args:
            wait_for_replication: If True, wait for stream to be REPLICATING before failback

        Returns:
            True if failback completed successfully, False otherwise
        """
        logger.info("=" * 60)
        logger.info("Starting Full Failback Process")
        logger.info("=" * 60)

        # Step 1: Verify clusters are healthy
        logger.info("\nStep 1: Verifying cluster health...")
        if not self.check_cluster_health(self.failback_primary_id):
            logger.error(f"Current active cluster ({self.failback_primary_id}) is not healthy")
            return False

        if not self.check_cluster_health(self.failback_standby_id):
            logger.error(f"Original primary cluster ({self.failback_standby_id}) is not healthy")
            logger.error("Please restore the original primary cluster before failback")
            return False

        logger.info("Both clusters are healthy")

        # Step 2: Create failback PCR stream
        logger.info("\nStep 2: Creating failback PCR stream...")
        stream_id = self.create_failback_pcr_stream()
        if not stream_id:
            logger.error("Failed to create failback PCR stream")
            return False

        # Step 3: Wait for replication (optional)
        if wait_for_replication:
            logger.info("\nStep 3: Waiting for replication to be ready...")
            if not self.wait_for_replication_ready():
                logger.error("Failback PCR stream did not become ready")
                return False
        else:
            logger.info("\nStep 3: Skipping replication readiness check")

        # Step 4: Initiate failback
        logger.info("\nStep 4: Initiating failback...")
        if not self.initiate_failback():
            logger.error("Failed to initiate failback")
            return False

        # Step 5: Monitor progress
        logger.info("\nStep 5: Monitoring failback progress...")
        success = self.monitor_failback_progress()

        if success:
            logger.info("\n" + "=" * 60)
            logger.info("FAILBACK COMPLETED SUCCESSFULLY!")
            logger.info("=" * 60)
            logger.info("\nIMPORTANT: Redirect application traffic back to original primary cluster")
            logger.info(f"   Original Primary: {self.failback_standby_id}")

        return success
