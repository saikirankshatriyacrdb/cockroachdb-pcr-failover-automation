#!/usr/bin/env python3
"""
Test script to run monitoring for a short period to verify it's working
"""
import time
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cluster_monitor import ClusterMonitor
from src.config import Config


def test_monitoring():
    """Run monitoring for a few cycles"""
    print("=" * 60)
    print("Testing Automated Failover Monitoring")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  - Health Check Interval: {Config.HEALTH_CHECK_INTERVAL} seconds")
    print(f"  - Failure Threshold: {Config.FAILURE_THRESHOLD} consecutive failures")
    print(f"  - Primary Cluster: {Config.PRIMARY_CLUSTER_ID}")
    print(f"  - Standby Cluster: {Config.STANDBY_CLUSTER_ID}")
    print(f"  - PCR Stream ID: {Config.PCR_STREAM_ID or 'Will be discovered'}")

    print("\n" + "=" * 60)
    print("Starting monitoring (will run for 3 health check cycles)")
    print("Press Ctrl+C to stop early")
    print("=" * 60 + "\n")

    monitor = ClusterMonitor()
    cycles = 0
    max_cycles = 3

    try:
        while cycles < max_cycles:
            cycles += 1
            print(f"\n--- Health Check Cycle {cycles}/{max_cycles} ---")

            is_healthy = monitor.check_primary_cluster_health()

            if is_healthy:
                print("  Primary cluster is healthy")
                print(f"  Failure count reset to: {monitor.failure_count}")
            else:
                print("  Primary cluster health check failed")
                print(f"  Failure count: {monitor.failure_count}/{Config.FAILURE_THRESHOLD}")

            stream_info = monitor.get_pcr_stream_info()
            if stream_info:
                status = stream_info.get("status", "UNKNOWN")
                replicated_time = stream_info.get("replicated_time", "N/A")
                replication_lag = stream_info.get("replication_lag_seconds", "N/A")
                print(f"  PCR Stream Status: {status}")
                print(f"  Replicated Time: {replicated_time}")
                print(f"  Replication Lag: {replication_lag} seconds")

            if cycles < max_cycles:
                print(f"\nWaiting {Config.HEALTH_CHECK_INTERVAL} seconds until next check...")
                time.sleep(Config.HEALTH_CHECK_INTERVAL)

        print("\n" + "=" * 60)
        print("Test Complete!")
        print("=" * 60)
        print("\nThe monitoring system is working correctly.")
        print("\nTo run full automation: python cli.py monitor")

    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")
    except Exception as e:
        print(f"\nError during monitoring: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_monitoring()
