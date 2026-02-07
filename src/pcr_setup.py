#!/usr/bin/env python3
"""
PCR Setup - Creates a PCR stream between primary and secondary clusters
Uses Config for defaults, or accepts command-line arguments
"""
import requests
import sys
import json
import time
from typing import Optional, Dict, Any
from src.config import Config


def create_pcr_stream(api_key: str, primary_cluster_id: str, standby_cluster_id: str) -> Optional[str]:
    """
    Create a PCR stream from primary to standby cluster

    Args:
        api_key: CockroachDB Cloud API secret key
        primary_cluster_id: Primary cluster ID
        standby_cluster_id: Standby cluster ID

    Returns:
        Stream ID if successful, None otherwise
    """
    url = f"{Config.API_BASE_URL}/physical-replication-streams"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "content-type": "application/json"
    }

    payload = {
        "primary_cluster_id": primary_cluster_id,
        "standby_cluster_id": standby_cluster_id
    }

    print("=" * 70)
    print("Creating PCR Stream")
    print("=" * 70)
    print(f"\nPrimary Cluster: {primary_cluster_id}")
    print(f"Standby Cluster: {standby_cluster_id}")
    print(f"\nPayload:")
    print(json.dumps(payload, indent=2))
    print("")

    try:
        print("Creating PCR stream...")
        response = requests.post(url, headers=headers, json=payload, timeout=30)

        if response.status_code in [200, 201, 202]:
            stream_data = response.json()
            stream_id = stream_data.get("id")
            status = stream_data.get("status", "UNKNOWN")

            print("PCR stream created successfully!")
            print(f"\nStream ID: {stream_id}")
            print(f"Status: {status}")
            print(f"\nFull response:")
            print(json.dumps(stream_data, indent=2))

            return stream_id
        else:
            print(f"Failed to create PCR stream: {response.status_code}")
            print(f"Response: {response.text}")
            return None

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def get_pcr_stream_info(api_key: str, stream_id: str) -> Optional[Dict[str, Any]]:
    """
    Get PCR stream information

    Args:
        api_key: CockroachDB Cloud API secret key
        stream_id: PCR stream ID

    Returns:
        Stream information dictionary or None
    """
    url = f"{Config.API_BASE_URL}/physical-replication-streams/{stream_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "content-type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get stream info: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error getting stream info: {str(e)}")
        return None


def monitor_stream_status(api_key: str, stream_id: str, max_wait_time: int = 300) -> bool:
    """
    Monitor PCR stream until it reaches REPLICATING status

    Args:
        api_key: CockroachDB Cloud API secret key
        stream_id: PCR stream ID
        max_wait_time: Maximum time to wait in seconds (default: 5 minutes)

    Returns:
        True if stream reaches REPLICATING status, False otherwise
    """
    print("\n" + "=" * 70)
    print("Monitoring PCR Stream Status")
    print("=" * 70)

    start_time = time.time()
    check_interval = 5  # Check every 5 seconds
    check_count = 0

    while time.time() - start_time < max_wait_time:
        check_count += 1
        stream_info = get_pcr_stream_info(api_key, stream_id)

        if stream_info:
            status = stream_info.get('status', 'UNKNOWN')
            elapsed = int(time.time() - start_time)

            print(f"[{check_count}] Status: {status} (elapsed: {elapsed}s)")

            if status == 'REPLICATING':
                print("\nPCR stream is now REPLICATING!")
                print("   Primary -> Secondary replication is active")

                if 'replicated_time' in stream_info:
                    print(f"   Replicated Time: {stream_info.get('replicated_time')}")

                return True
            elif status in ['FAILED', 'ERROR', 'CANCELLED']:
                print(f"\nPCR stream failed with status: {status}")
                if 'error_message' in stream_info:
                    print(f"   Error: {stream_info.get('error_message')}")
                return False
            else:
                time.sleep(check_interval)
        else:
            print(f"[{check_count}] Could not retrieve stream status")
            time.sleep(check_interval)

    print(f"\nTimeout after {max_wait_time} seconds")
    print("   Stream may still be initializing. Check status manually.")
    return False


def run_setup(api_key: str = None, primary: str = None, secondary: str = None,
              no_monitor: bool = False, max_wait: int = 300, skip_confirm: bool = False):
    """
    Run the PCR setup process.

    Args:
        api_key: API key (defaults to Config.API_SECRET_KEY)
        primary: Primary cluster ID (defaults to Config.PRIMARY_CLUSTER_ID)
        secondary: Secondary cluster ID (defaults to Config.STANDBY_CLUSTER_ID)
        no_monitor: Skip monitoring stream status
        max_wait: Maximum time to wait for REPLICATING status
        skip_confirm: Skip confirmation prompt
    """
    api_key = api_key or Config.API_SECRET_KEY
    primary = primary or Config.PRIMARY_CLUSTER_ID
    secondary = secondary or Config.STANDBY_CLUSTER_ID

    if not api_key:
        print("Error: API key required. Set COCKROACH_API_SECRET_KEY or pass --api-key")
        sys.exit(1)
    if not primary:
        print("Error: Primary cluster ID required. Set PRIMARY_CLUSTER_ID or pass --primary")
        sys.exit(1)
    if not secondary:
        print("Error: Secondary cluster ID required. Set STANDBY_CLUSTER_ID or pass --secondary")
        sys.exit(1)

    print("PCR Stream Setup")
    print("=" * 70)
    print(f"\nThis will create a PCR stream:")
    print(f"  FROM (Primary):   {primary}")
    print(f"  TO (Secondary):  {secondary}")
    print("")

    if not skip_confirm:
        confirm = input("Continue? (yes/no): ")
        if confirm.lower() not in ['yes', 'y']:
            print("Cancelled")
            return

    # Create PCR stream
    stream_id = create_pcr_stream(api_key, primary, secondary)

    if not stream_id:
        print("\nFailed to create PCR stream")
        sys.exit(1)

    # Monitor stream status if requested
    if not no_monitor:
        success = monitor_stream_status(api_key, stream_id, max_wait)
        if not success:
            print("\nStream may still be initializing. Check status later.")

    # Print summary
    print("\n" + "=" * 70)
    print("Setup Complete")
    print("=" * 70)
    print(f"\nPCR Stream ID: {stream_id}")
    print(f"\nTo use this stream in your automation:")
    print(f"  1. Add to .env file: PCR_STREAM_ID={stream_id}")
    print(f"  2. Or set environment variable: export PCR_STREAM_ID={stream_id}")
    print(f"\nTo check stream status:")
    print(f"   python cli.py status")
    print("\n" + "=" * 70)
