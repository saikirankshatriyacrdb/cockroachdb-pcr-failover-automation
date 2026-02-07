#!/usr/bin/env python3
"""
Complete failover test scenario
Orchestrates a full test: disrupt -> monitor -> restore
"""
import time
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cluster_monitor import ClusterMonitor
from src.config import Config
from src.simulate import disrupt_cluster, restore_cluster, get_cluster_info


def test_failover_scenario():
    """Run complete failover test scenario"""
    print("=" * 60)
    print("CockroachDB Automated Failover Test Scenario")
    print("=" * 60)

    print("\nWARNING: This will disrupt your PRIMARY cluster!")
    print("   1. Disrupt the primary cluster")
    print("   2. Monitor for automatic failover")
    print("   3. Restore the primary cluster")
    print("\n   Make sure you're testing in a non-production environment!")

    confirm = input("\n   Type 'TEST' to proceed: ")
    if confirm != 'TEST':
        print("Test cancelled")
        return

    monitor = ClusterMonitor()

    # Step 1: Pre-test status
    print("\n" + "=" * 60)
    print("Step 1: Pre-Test Status Check")
    print("=" * 60)
    print("\nChecking primary cluster status...")
    cluster_info = get_cluster_info(Config.PRIMARY_CLUSTER_ID)
    if cluster_info:
        print(f"   Cluster: {cluster_info.get('name', 'N/A')}")
        print(f"   State: {cluster_info.get('state', 'N/A')}")
        print(f"   Operation Status: {cluster_info.get('operation_status', 'N/A')}")

    print("\nChecking PCR stream status...")
    stream_info = monitor.get_pcr_stream_info()
    if stream_info:
        print(f"   Stream ID: {stream_info.get('id', 'N/A')}")
        print(f"   Status: {stream_info.get('status', 'N/A')}")
        print(f"   Replication Lag: {stream_info.get('replication_lag_seconds', 'N/A')} seconds")

    # Step 2: Trigger disruption
    print("\n" + "=" * 60)
    print("Step 2: Triggering Cluster Disruption")
    print("=" * 60)
    print("\nDisrupting primary cluster...")
    disruption_success = disrupt_cluster(Config.PRIMARY_CLUSTER_ID)

    if not disruption_success:
        print("Failed to trigger disruption. Aborting test.")
        return

    # Step 3: Monitor for failures
    print("\n" + "=" * 60)
    print("Step 3: Monitoring for Failover Trigger")
    print("=" * 60)
    print(f"\n   Failure threshold: {Config.FAILURE_THRESHOLD} consecutive failures")
    print(f"   Health check interval: {Config.HEALTH_CHECK_INTERVAL} seconds")
    print(f"   Failover delay: {Config.FAILOVER_DELAY} seconds")

    max_wait = (Config.FAILURE_THRESHOLD * Config.HEALTH_CHECK_INTERVAL) + Config.FAILOVER_DELAY + 60
    start_time = time.time()
    check_count = 0

    try:
        while time.time() - start_time < max_wait:
            check_count += 1
            elapsed = int(time.time() - start_time)

            print(f"\n--- Health Check #{check_count} (Elapsed: {elapsed}s) ---")

            is_healthy = monitor.check_primary_cluster_health()

            if is_healthy:
                print("  Cluster is healthy")
            else:
                print("  Cluster health check FAILED")
            print(f"  Failure count: {monitor.failure_count}/{Config.FAILURE_THRESHOLD}")

            stream_info = monitor.get_pcr_stream_info()
            if stream_info:
                status = stream_info.get("status", "UNKNOWN")
                print(f"  PCR Stream Status: {status}")

                if status == "FAILING_OVER":
                    print("\nFAILOVER DETECTED! Stream is in FAILING_OVER status")
                    print("   Waiting for completion...")

                    while time.time() - start_time < max_wait:
                        time.sleep(10)
                        stream_info = monitor.get_pcr_stream_info()
                        if stream_info:
                            status = stream_info.get("status", "UNKNOWN")
                            print(f"   Status: {status}")
                            if status == "COMPLETED":
                                activated_at = stream_info.get("activated_at", "N/A")
                                print(f"\nFAILOVER COMPLETED at {activated_at}!")
                                break
                    break
                elif status == "COMPLETED":
                    print("\nFAILOVER ALREADY COMPLETED!")
                    break

            if check_count < max_wait // Config.HEALTH_CHECK_INTERVAL:
                print(f"\n   Waiting {Config.HEALTH_CHECK_INTERVAL} seconds until next check...")
                time.sleep(Config.HEALTH_CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\n\nMonitoring interrupted by user")

    # Step 4: Restore cluster
    print("\n" + "=" * 60)
    print("Step 4: Restoring Primary Cluster")
    print("=" * 60)
    print("\nRestoring cluster to normal operation...")

    restore_confirm = input("   Restore cluster now? (yes/no): ")
    if restore_confirm.lower() in ['yes', 'y']:
        restore_success = restore_cluster(Config.PRIMARY_CLUSTER_ID)

        if restore_success:
            print("\nWaiting 10 seconds for cluster to recover...")
            time.sleep(10)

            print("\nChecking cluster status after restoration...")
            cluster_info = get_cluster_info(Config.PRIMARY_CLUSTER_ID)
            if cluster_info:
                print(f"   State: {cluster_info.get('state', 'N/A')}")
                print(f"   Operation Status: {cluster_info.get('operation_status', 'N/A')}")
        else:
            print("Failed to restore cluster. Please restore manually.")
    else:
        print("Skipping restoration. Please restore manually when ready.")

    # Summary
    print("\n" + "=" * 60)
    print("Test Scenario Complete")
    print("=" * 60)
    print("\nNext steps:")
    print("   1. Review the failover logs")
    print("   2. Verify standby cluster is ready for traffic")
    print("   3. If needed, set up new PCR stream for failback")


if __name__ == "__main__":
    try:
        test_failover_scenario()
    except Exception as e:
        print(f"\nError during test: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
