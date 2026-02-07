#!/usr/bin/env python3
"""
Simulate cluster failure using CockroachDB Cloud disruption API
"""
import requests
import json
import sys
import os
import time
from src.config import Config


def get_cluster_info(cluster_id):
    """Get current cluster information"""
    url = f"{Config.API_BASE_URL}/clusters/{cluster_id}"
    headers = {
        "Authorization": f"Bearer {Config.API_SECRET_KEY}",
        "content-type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting cluster info: {response.status_code}")
            print(response.text)
            return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None


def disrupt_cluster(cluster_id, disruption_file=None):
    """Trigger cluster disruption"""
    url = f"{Config.API_BASE_URL}/clusters/{cluster_id}/disrupt"
    headers = {
        "Authorization": f"Bearer {Config.API_SECRET_KEY}",
        "content-type": "application/json"
    }

    # Default disruption file location: examples/disruption.json relative to project root
    if disruption_file is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        disruption_file = os.path.join(project_root, "examples", "disruption.json")

    try:
        # Read disruption configuration
        with open(disruption_file, 'r') as f:
            disruption_data = json.load(f)

        print(f"\nDisruption Configuration:")
        print(json.dumps(disruption_data, indent=2))

        print(f"\nTriggering disruption on cluster {cluster_id}...")
        response = requests.put(url, headers=headers, json=disruption_data, timeout=30)

        if response.status_code in [200, 202]:
            print("Disruption triggered successfully!")
            print(f"   Response: {response.text}")
            return True
        else:
            print(f"Failed to trigger disruption: {response.status_code}")
            print(f"   Response: {response.text}")
            return False

    except FileNotFoundError:
        print(f"Disruption file not found: {disruption_file}")
        return False
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in disruption file: {str(e)}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def restore_cluster(cluster_id):
    """Clear disruptions and restore normal operation"""
    url = f"{Config.API_BASE_URL}/clusters/{cluster_id}/disrupt"
    headers = {
        "Authorization": f"Bearer {Config.API_SECRET_KEY}",
        "content-type": "application/json"
    }

    try:
        print(f"\nRestoring cluster {cluster_id} (clearing disruptions)...")
        # Empty body to clear disruptions
        response = requests.put(url, headers=headers, json={}, timeout=30)

        if response.status_code in [200, 202]:
            print("Cluster restoration initiated!")
            print(f"   Response: {response.text}")
            return True
        else:
            print(f"Failed to restore cluster: {response.status_code}")
            print(f"   Response: {response.text}")
            return False

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def run_simulate(action, cluster_id=None, disruption_file=None, skip_confirm=False):
    """
    Run simulation action.

    Args:
        action: 'disrupt', 'restore', or 'status'
        cluster_id: Cluster ID (defaults to Config.PRIMARY_CLUSTER_ID)
        disruption_file: Path to disruption JSON file
        skip_confirm: Skip confirmation prompts
    """
    cluster_id = cluster_id or Config.PRIMARY_CLUSTER_ID

    print("=" * 60)
    print("CockroachDB Cluster Disruption Tool")
    print("=" * 60)
    print(f"\nCluster ID: {cluster_id}")

    if action == 'status':
        print("\nGetting cluster status...")
        cluster_info = get_cluster_info(cluster_id)
        if cluster_info:
            print(f"\n  Cluster Name: {cluster_info.get('name', 'N/A')}")
            print(f"  State: {cluster_info.get('state', 'N/A')}")
            print(f"  Operation Status: {cluster_info.get('operation_status', 'N/A')}")
            print(f"  Regions: {[r.get('name') for r in cluster_info.get('regions', [])]}")

    elif action == 'disrupt':
        print("\nWARNING: This will disrupt the cluster!")
        print("   This is for TESTING purposes only.")
        if not skip_confirm:
            confirm = input("\n   Type 'YES' to confirm: ")
            if confirm != 'YES':
                print("Cancelled")
                return
        else:
            print("\n   Skipping confirmation (--yes flag used)")

        # Show cluster status before
        print("\nCluster status BEFORE disruption:")
        cluster_info = get_cluster_info(cluster_id)
        if cluster_info:
            print(f"   State: {cluster_info.get('state', 'N/A')}")
            print(f"   Operation Status: {cluster_info.get('operation_status', 'N/A')}")

        # Trigger disruption
        success = disrupt_cluster(cluster_id, disruption_file)

        if success:
            print("\nWaiting 5 seconds, then checking status...")
            time.sleep(5)

            # Check status after
            print("\nCluster status AFTER disruption:")
            cluster_info = get_cluster_info(cluster_id)
            if cluster_info:
                print(f"   State: {cluster_info.get('state', 'N/A')}")
                print(f"   Operation Status: {cluster_info.get('operation_status', 'N/A')}")

            print("\nThe failover automation should detect this failure")
            print("   Monitor with: python cli.py monitor")

    elif action == 'restore':
        print("\nRestoring cluster to normal operation...")
        success = restore_cluster(cluster_id)

        if success:
            print("\nWaiting 5 seconds, then checking status...")
            time.sleep(5)

            # Check status after
            print("\nCluster status AFTER restoration:")
            cluster_info = get_cluster_info(cluster_id)
            if cluster_info:
                print(f"   State: {cluster_info.get('state', 'N/A')}")
                print(f"   Operation Status: {cluster_info.get('operation_status', 'N/A')}")
