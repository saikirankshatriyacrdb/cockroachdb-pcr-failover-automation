#!/usr/bin/env python3
"""
Debug script to see actual API responses
"""
import requests
import json
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import Config


def debug_api():
    """Debug API responses"""
    headers = {
        "Authorization": f"Bearer {Config.API_SECRET_KEY}",
        "content-type": "application/json"
    }

    print("=" * 60)
    print("Debugging CockroachDB Cloud API")
    print("=" * 60)

    # Test primary cluster endpoint
    print("\n1. Testing Primary Cluster Endpoint...")
    primary_url = f"{Config.API_BASE_URL}/clusters/{Config.PRIMARY_CLUSTER_ID}"
    print(f"URL: {primary_url}")

    try:
        response = requests.get(primary_url, headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print("\nResponse JSON:")
            print(json.dumps(data, indent=2))

            if 'status' in data:
                print(f"\nFound 'status' field: {data['status']}")
            else:
                print("\nNo 'status' field found. Available fields:")
                print(list(data.keys()))
        else:
            print(f"\nError response:")
            print(response.text)
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

    # Test standby cluster endpoint
    print("\n\n2. Testing Standby Cluster Endpoint...")
    standby_url = f"{Config.API_BASE_URL}/clusters/{Config.STANDBY_CLUSTER_ID}"
    print(f"URL: {standby_url}")

    try:
        response = requests.get(standby_url, headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print("\nResponse JSON:")
            print(json.dumps(data, indent=2))
        else:
            print(f"\nError response:")
            print(response.text)
    except Exception as e:
        print(f"Error: {str(e)}")

    # Test PCR streams endpoint with primary cluster ID
    print("\n\n3. Testing PCR Streams Endpoint (with Primary Cluster ID)...")
    streams_url = f"{Config.API_BASE_URL}/physical-replication-streams"
    params = {"cluster_id": Config.PRIMARY_CLUSTER_ID}
    print(f"URL: {streams_url}")
    print(f"Params: {params}")

    try:
        response = requests.get(streams_url, headers=headers, params=params, timeout=10)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print("\nResponse JSON:")
            print(json.dumps(data, indent=2))

            if isinstance(data, list):
                print(f"\nFound {len(data)} stream(s)")
            elif isinstance(data, dict):
                if 'streams' in data:
                    print(f"\nFound {len(data['streams'])} stream(s) in 'streams' field")
                else:
                    print("\nResponse is dict but no 'streams' field. Keys:")
                    print(list(data.keys()))
        else:
            print(f"\nError response:")
            print(response.text)
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

    # Test PCR streams endpoint with standby cluster ID
    print("\n\n4. Testing PCR Streams Endpoint (with Standby Cluster ID)...")
    params = {"cluster_id": Config.STANDBY_CLUSTER_ID}
    print(f"Params: {params}")

    try:
        response = requests.get(streams_url, headers=headers, params=params, timeout=10)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print("\nResponse JSON:")
            print(json.dumps(data, indent=2))
        else:
            print(f"\nError response:")
            print(response.text)
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    debug_api()
