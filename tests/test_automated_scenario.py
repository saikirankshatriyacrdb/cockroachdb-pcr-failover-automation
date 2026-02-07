#!/usr/bin/env python3
"""
Automated Test Scenario with Scheduled Actions
- Starts continuous monitoring
- After 2 minutes: Triggers disruption on primary
- After 4 minutes: Checks replication lag and triggers failover to consistent point
"""
import time
import signal
import sys
import os
from datetime import datetime, timedelta
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cluster_monitor import ClusterMonitor
from src.config import Config
from src.simulate import disrupt_cluster, get_cluster_info

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AutomatedTestScenario:
    """Automated test scenario with scheduled actions"""

    def __init__(self):
        self.monitor = ClusterMonitor()
        self.running = True
        self.start_time = None
        self.disruption_triggered = False
        self.failover_triggered = False

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        logger.info("Received shutdown signal. Stopping test...")
        self.running = False

    def get_replication_info(self) -> dict:
        stream_info = self.monitor.get_pcr_stream_info()
        if stream_info:
            return {
                "status": stream_info.get("status", "UNKNOWN"),
                "replicated_time": stream_info.get("replicated_time", "N/A"),
                "replication_lag_seconds": stream_info.get("replication_lag_seconds", 0),
                "retained_time": stream_info.get("retained_time", "N/A")
            }
        return None

    def calculate_consistent_failover_time(self, replication_lag_seconds: int) -> str:
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        buffer_seconds = 10
        failover_time = now - timedelta(seconds=replication_lag_seconds + buffer_seconds)
        return failover_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def trigger_disruption(self):
        if self.disruption_triggered:
            return

        elapsed = time.time() - self.start_time
        logger.info(f"\n{'='*60}")
        logger.info(f"T+{elapsed:.1f}s - Triggering Disruption")
        logger.info(f"{'='*60}\n")

        logger.info("Pre-disruption status:")
        cluster_info = get_cluster_info(Config.PRIMARY_CLUSTER_ID)
        if cluster_info:
            logger.info(f"   Primary State: {cluster_info.get('state', 'N/A')}")
            logger.info(f"   Operation Status: {cluster_info.get('operation_status', 'N/A')}")

        rep_info = self.get_replication_info()
        if rep_info:
            logger.info(f"   PCR Stream Status: {rep_info['status']}")
            logger.info(f"   Replication Lag: {rep_info['replication_lag_seconds']} seconds")

        logger.info("\nTriggering disruption on primary cluster...")
        success = disrupt_cluster(Config.PRIMARY_CLUSTER_ID)

        if success:
            self.disruption_triggered = True
            logger.info("Disruption triggered successfully!")
            time.sleep(5)
            cluster_info = get_cluster_info(Config.PRIMARY_CLUSTER_ID)
            if cluster_info:
                logger.info(f"\nPost-disruption status:")
                logger.info(f"   Primary State: {cluster_info.get('state', 'N/A')}")
                logger.info(f"   Operation Status: {cluster_info.get('operation_status', 'N/A')}")
        else:
            logger.error("Failed to trigger disruption")

    def trigger_failover_with_consistency(self):
        if self.failover_triggered:
            return

        elapsed = time.time() - self.start_time
        logger.info(f"\n{'='*60}")
        logger.info(f"T+{elapsed:.1f}s - Triggering Failover")
        logger.info(f"{'='*60}\n")

        rep_info = self.get_replication_info()
        if not rep_info:
            logger.error("Could not get replication information")
            return

        logger.info(f"   PCR Stream Status: {rep_info['status']}")
        logger.info(f"   Replicated Time: {rep_info['replicated_time']}")
        logger.info(f"   Replication Lag: {rep_info['replication_lag_seconds']} seconds")

        if rep_info['status'] not in ['REPLICATING', 'STARTING']:
            logger.error(f"Cannot failover: PCR stream is in {rep_info['status']} status")
            return

        replication_lag = rep_info['replication_lag_seconds']

        if replication_lag > 0:
            replicated_time_str = rep_info['replicated_time']
            if replicated_time_str and replicated_time_str != "0001-01-01T00:00:00Z":
                logger.info(f"\nInitiating failover to consistent point: {replicated_time_str}")
                success = self.monitor.initiate_failover(failover_at=replicated_time_str)
            else:
                logger.info("\nReplicated time not available, using latest consistent time")
                success = self.monitor.initiate_failover()
        else:
            logger.info("\nNo replication lag, using latest consistent time")
            success = self.monitor.initiate_failover()

        if success:
            self.failover_triggered = True
            logger.info("Failover initiated successfully!")
            self.monitor.monitor_failover_progress()
            logger.info("\n" + "="*60)
            logger.info("FAILOVER COMPLETED!")
            logger.info("="*60)
            logger.info("\nIMPORTANT: Redirect application traffic to standby cluster")
        else:
            logger.error("Failed to initiate failover")

    def monitoring_loop(self):
        logger.info("Starting continuous monitoring...")
        check_count = 0

        while self.running:
            check_count += 1
            elapsed = time.time() - self.start_time

            is_healthy = self.monitor.check_primary_cluster_health()
            rep_info = self.get_replication_info()

            status_line = f"[{elapsed:6.1f}s] Check #{check_count:3d} | "
            status_line += f"Primary: {'OK' if is_healthy else 'FAIL'} | "
            if rep_info:
                status_line += f"PCR: {rep_info['status']:12s} | "
                status_line += f"Lag: {rep_info['replication_lag_seconds']:3d}s"
            else:
                status_line += "PCR: N/A"
            logger.info(status_line)

            if elapsed >= 120 and not self.disruption_triggered:
                self.trigger_disruption()

            if elapsed >= 240 and not self.failover_triggered:
                self.trigger_failover_with_consistency()

            time.sleep(Config.HEALTH_CHECK_INTERVAL)

    def run(self):
        logger.info("="*60)
        logger.info("Automated Test Scenario")
        logger.info("="*60)
        logger.info(f"\nConfiguration:")
        logger.info(f"  Primary Cluster: {Config.PRIMARY_CLUSTER_ID}")
        logger.info(f"  Standby Cluster: {Config.STANDBY_CLUSTER_ID}")
        logger.info(f"  Health Check Interval: {Config.HEALTH_CHECK_INTERVAL} seconds")
        logger.info(f"\nTest Timeline:")
        logger.info(f"  T+0:00   - Start continuous monitoring")
        logger.info(f"  T+2:00   - Trigger disruption on primary")
        logger.info(f"  T+4:00   - Check replication lag and trigger failover")
        logger.info(f"\nPress Ctrl+C to stop early")
        logger.info("="*60 + "\n")

        self.start_time = time.time()

        try:
            self.monitoring_loop()
        except KeyboardInterrupt:
            logger.info("\n\nTest interrupted by user")
        except Exception as e:
            logger.error(f"\nError during test: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            elapsed = time.time() - self.start_time
            logger.info("\n" + "="*60)
            logger.info("Test Scenario Complete")
            logger.info("="*60)
            logger.info(f"Total duration: {elapsed:.1f} seconds")
            logger.info(f"Disruption triggered: {'yes' if self.disruption_triggered else 'no'}")
            logger.info(f"Failover triggered: {'yes' if self.failover_triggered else 'no'}")


if __name__ == "__main__":
    scenario = AutomatedTestScenario()
    scenario.run()
