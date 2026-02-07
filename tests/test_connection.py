#!/usr/bin/env python3
"""
Test script to verify CockroachDB Cloud API connection and cluster status
"""
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cluster_monitor import ClusterMonitor
from src.config import Config


def test_connection():
    """Test API connection and cluster status"""
    print("=" * 60)
    print("Testing CockroachDB Cloud API Connection")
    print("=" * 60)

    # Verify configuration
    print("\n1. Checking Configuration...")
    if not Config.API_SECRET_KEY:
        print("ERROR: API_SECRET_KEY not set in .env file")
        return False
    else:
        print(f"  API Key configured (length: {len(Config.API_SECRET_KEY)} chars)")

    if not Config.PRIMARY_CLUSTER_ID:
        print("ERROR: PRIMARY_CLUSTER_ID not set")
        return False
    else:
        print(f"  Primary Cluster ID: {Config.PRIMARY_CLUSTER_ID}")

    if not Config.STANDBY_CLUSTER_ID:
        print("ERROR: STANDBY_CLUSTER_ID not set")
        return False
    else:
        print(f"  Standby Cluster ID: {Config.STANDBY_CLUSTER_ID}")

    # Initialize monitor
    print("\n2. Initializing ClusterMonitor...")
    monitor = ClusterMonitor()
    print("  ClusterMonitor initialized")

    # Test primary cluster health
    print("\n3. Checking Primary Cluster Health...")
    try:
        is_healthy = monitor.check_primary_cluster_health()
        if is_healthy:
            print("  Primary cluster is HEALTHY")
        else:
            print("  Primary cluster is UNHEALTHY or unreachable")
    except Exception as e:
        print(f"  Error checking primary cluster: {str(e)}")
        return False

    # Get PCR stream info
    print("\n4. Checking PCR Stream Status...")
    try:
        stream_info = monitor.get_pcr_stream_info()
        if stream_info:
            print(f"  PCR Stream found!")
            print(f"  - Stream ID: {stream_info.get('id', 'N/A')}")
            print(f"  - Status: {stream_info.get('status', 'N/A')}")
            print(f"  - Primary Cluster: {stream_info.get('primary_cluster_id', 'N/A')}")
            print(f"  - Standby Cluster: {stream_info.get('standby_cluster_id', 'N/A')}")
            print(f"  - Created At: {stream_info.get('created_at', 'N/A')}")

            if not Config.PCR_STREAM_ID and stream_info.get('id'):
                print(f"\n  Tip: Add PCR_STREAM_ID={stream_info.get('id')} to .env for faster startup")
        else:
            print("  No PCR stream found or stream ID not discovered")
    except Exception as e:
        print(f"  Could not retrieve PCR stream info: {str(e)}")

    print("\n" + "=" * 60)
    print("Connection Test Complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. If all checks passed, run: python cli.py monitor")
    print("2. To test failover: python cli.py simulate disrupt")

    return True


if __name__ == "__main__":
    try:
        test_connection()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
