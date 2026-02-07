#!/usr/bin/env python3
"""
Route 53 DNS Failover Setup for CockroachDB PCR
Uses Route 53 health checks and DNS failover for seamless routing
"""
import os
import sys
import time
import logging
from typing import Optional, Dict, Any

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Route53FailoverManager:
    """Manages Route 53 DNS failover for CockroachDB clusters"""

    def __init__(self, hosted_zone_id: str, domain_name: str):
        """
        Initialize Route 53 manager

        Args:
            hosted_zone_id: Route 53 hosted zone ID
            domain_name: Domain name for the database endpoint (e.g., db.example.com)
        """
        import boto3
        self.route53 = boto3.client('route53')
        self.hosted_zone_id = hosted_zone_id
        self.domain_name = domain_name

        # Cluster DNS endpoints from environment
        self.primary_dns = os.getenv("PRIMARY_SQL_DNS", "<YOUR_PRIMARY_SQL_DNS>")
        self.standby_dns = os.getenv("STANDBY_SQL_DNS", "<YOUR_STANDBY_SQL_DNS>")

    def create_health_check(self, cluster_dns: str, cluster_name: str) -> Optional[str]:
        """
        Create a Route 53 health check for a cluster

        Args:
            cluster_dns: Cluster DNS endpoint
            cluster_name: Name identifier for the cluster

        Returns:
            Health check ID
        """
        try:
            response = self.route53.create_health_check(
                CallerReference=f"{cluster_name}-{int(time.time())}",
                HealthCheckConfig={
                    'Type': 'HTTPS',
                    'ResourcePath': '/health',
                    'FullyQualifiedDomainName': cluster_dns,
                    'Port': 443,
                    'RequestInterval': 30,
                    'FailureThreshold': 3,
                    'EnableSNI': True
                }
            )

            health_check_id = response['HealthCheck']['Id']
            logger.info(f"Created health check for {cluster_name}: {health_check_id}")
            return health_check_id

        except Exception as e:
            logger.error(f"Failed to create health check: {str(e)}")
            return None

    def create_failover_record(self, cluster_dns: str, health_check_id: str,
                              is_primary: bool = True):
        """
        Create a Route 53 DNS record with failover routing

        Args:
            cluster_dns: Cluster DNS endpoint
            health_check_id: Route 53 health check ID
            is_primary: True for primary (PRIMARY), False for standby (SECONDARY)
        """
        try:
            record_type = 'PRIMARY' if is_primary else 'SECONDARY'

            response = self.route53.change_resource_record_sets(
                HostedZoneId=self.hosted_zone_id,
                ChangeBatch={
                    'Changes': [{
                        'Action': 'UPSERT',
                        'ResourceRecordSet': {
                            'Name': self.domain_name,
                            'Type': 'CNAME',
                            'SetIdentifier': f'{record_type}-{cluster_dns}',
                            'TTL': 60,
                            'ResourceRecords': [{'Value': cluster_dns}],
                            'HealthCheckId': health_check_id,
                            'Failover': record_type
                        }
                    }]
                }
            )

            logger.info(f"Created {record_type} failover record for {cluster_dns}")
            return response['ChangeInfo']['Id']

        except Exception as e:
            logger.error(f"Failed to create failover record: {str(e)}")
            return None

    def setup_failover_dns(self):
        """Set up complete Route 53 failover configuration"""
        logger.info("Setting up Route 53 DNS failover...")

        # Create health checks
        primary_health_check = self.create_health_check(self.primary_dns, "primary")
        standby_health_check = self.create_health_check(self.standby_dns, "standby")

        if not primary_health_check or not standby_health_check:
            logger.error("Failed to create health checks")
            return False

        # Create DNS records
        primary_record = self.create_failover_record(self.primary_dns, primary_health_check, is_primary=True)
        standby_record = self.create_failover_record(self.standby_dns, standby_health_check, is_primary=False)

        if primary_record and standby_record:
            logger.info("Route 53 failover DNS setup complete")
            logger.info(f"   Applications should connect to: {self.domain_name}")
            return True

        return False
