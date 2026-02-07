#!/usr/bin/env python3
"""
Connection Manager - Provides seamless failover for applications
Manages connection strings and automatically routes to active cluster
"""
import os
import time
import logging
import requests
from typing import Optional, Dict, Any
from src.config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages database connections with automatic failover"""

    def __init__(self):
        self.api_key = Config.API_SECRET_KEY
        self.base_url = Config.API_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "content-type": "application/json"
        }
        self.primary_cluster_id = Config.PRIMARY_CLUSTER_ID
        self.standby_cluster_id = Config.STANDBY_CLUSTER_ID
        self.current_active_cluster = None
        self.cluster_connection_strings = {}
        self.last_health_check = {}
        self.health_check_interval = 30  # seconds

    def get_cluster_connection_info(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        """Get cluster connection information from API"""
        try:
            url = f"{self.base_url}/clusters/{cluster_id}"
            response = requests.get(url, headers=self.headers, timeout=10)

            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"Error getting cluster info for {cluster_id}: {str(e)}")
            return None

    def get_cluster_sql_dns(self, cluster_id: str) -> Optional[str]:
        """Get SQL DNS endpoint for a cluster"""
        cluster_info = self.get_cluster_connection_info(cluster_id)
        if cluster_info:
            return cluster_info.get('sql_dns')
        return None

    def check_cluster_health(self, cluster_id: str) -> bool:
        """Check if cluster is healthy and accepting connections"""
        try:
            cluster_info = self.get_cluster_connection_info(cluster_id)
            if not cluster_info:
                return False

            state = cluster_info.get("state", "").upper()
            operation_status = cluster_info.get("operation_status", "").upper()

            healthy_states = ["CREATED"]
            if state in healthy_states and operation_status != "FAILED":
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking cluster health: {str(e)}")
            return False

    def get_active_cluster(self, force_check: bool = False) -> str:
        """
        Determine which cluster is currently active.
        Returns cluster ID of the active cluster.
        """
        current_time = time.time()

        # Check if we need to refresh health status
        if (force_check or
            self.current_active_cluster is None or
            current_time - self.last_health_check.get(self.primary_cluster_id, 0) > self.health_check_interval):

            # Check primary cluster
            primary_healthy = self.check_cluster_health(self.primary_cluster_id)
            self.last_health_check[self.primary_cluster_id] = current_time

            if primary_healthy:
                self.current_active_cluster = self.primary_cluster_id
                logger.info(f"Primary cluster ({self.primary_cluster_id}) is active")
                return self.primary_cluster_id

            # Primary is down, check standby
            standby_healthy = self.check_cluster_health(self.standby_cluster_id)
            self.last_health_check[self.standby_cluster_id] = current_time

            if standby_healthy:
                self.current_active_cluster = self.standby_cluster_id
                logger.warning(f"Primary cluster is down, using standby ({self.standby_cluster_id})")
                return self.standby_cluster_id

            # Both clusters are down
            logger.error("Both clusters are down!")
            return self.primary_cluster_id

        return self.current_active_cluster or self.primary_cluster_id

    def get_connection_string(self, username: str, password: str, database: str = "defaultdb",
                             use_ssl: bool = True) -> str:
        """
        Get connection string for the currently active cluster.

        Args:
            username: Database username
            password: Database password
            database: Database name (default: defaultdb)
            use_ssl: Use SSL connection (default: True)

        Returns:
            PostgreSQL connection string
        """
        active_cluster_id = self.get_active_cluster()
        sql_dns = self.get_cluster_sql_dns(active_cluster_id)

        if not sql_dns:
            logger.error(f"Could not get SQL DNS for cluster {active_cluster_id}")
            if Config.PRIMARY_CLUSTER_ENDPOINT:
                return Config.PRIMARY_CLUSTER_ENDPOINT
            return None

        ssl_mode = "require" if use_ssl else "disable"
        connection_string = f"postgresql://{username}:{password}@{sql_dns}:26257/{database}?sslmode={ssl_mode}"

        logger.info(f"Generated connection string for cluster: {active_cluster_id}")
        return connection_string

    def get_all_connection_strings(self, username: str, password: str,
                                   database: str = "defaultdb", use_ssl: bool = True) -> Dict[str, str]:
        """
        Get connection strings for both clusters.
        Useful for connection pooling with multiple hosts.

        Returns:
            Dictionary with 'primary' and 'standby' connection strings
        """
        primary_dns = self.get_cluster_sql_dns(self.primary_cluster_id)
        standby_dns = self.get_cluster_sql_dns(self.standby_cluster_id)

        ssl_mode = "require" if use_ssl else "disable"

        connections = {}

        if primary_dns:
            connections['primary'] = f"postgresql://{username}:{password}@{primary_dns}:26257/{database}?sslmode={ssl_mode}"

        if standby_dns:
            connections['standby'] = f"postgresql://{username}:{password}@{standby_dns}:26257/{database}?sslmode={ssl_mode}"

        return connections

    def get_multi_host_connection_string(self, username: str, password: str,
                                        database: str = "defaultdb", use_ssl: bool = True) -> str:
        """
        Get a multi-host connection string that PostgreSQL drivers can use for automatic failover.
        Some drivers (like psycopg2) support multiple hosts in connection string.

        Format: postgresql://user:pass@host1:port,host2:port/db?sslmode=require
        """
        primary_dns = self.get_cluster_sql_dns(self.primary_cluster_id)
        standby_dns = self.get_cluster_sql_dns(self.standby_cluster_id)

        if not primary_dns or not standby_dns:
            logger.warning("Could not get DNS for both clusters, using single host")
            return self.get_connection_string(username, password, database, use_ssl)

        ssl_mode = "require" if use_ssl else "disable"
        multi_host = f"postgresql://{username}:{password}@{primary_dns}:26257,{standby_dns}:26257/{database}?sslmode={ssl_mode}"

        return multi_host


class ApplicationConnectionHelper:
    """Helper class for applications to get database connections"""

    def __init__(self):
        self.manager = ConnectionManager()
        # Cache connection strings (refresh periodically)
        self.connection_cache = {}
        self.cache_ttl = 60  # seconds
        self.last_cache_update = 0

    def get_db_connection_string(self, username: Optional[str] = None,
                                password: Optional[str] = None) -> str:
        """
        Get connection string for applications.
        Uses environment variables if username/password not provided.
        """
        username = username or os.getenv("DB_USERNAME") or os.getenv("COCKROACH_USER")
        password = password or os.getenv("DB_PASSWORD") or os.getenv("COCKROACH_PASSWORD")

        if not username or not password:
            raise ValueError("Database username and password required (set DB_USERNAME/DB_PASSWORD env vars)")

        # Check cache
        current_time = time.time()
        cache_key = f"{username}@{self.manager.get_active_cluster()}"

        if (cache_key in self.connection_cache and
            current_time - self.last_cache_update < self.cache_ttl):
            return self.connection_cache[cache_key]

        # Get fresh connection string
        conn_str = self.manager.get_connection_string(username, password)
        self.connection_cache[cache_key] = conn_str
        self.last_cache_update = current_time

        return conn_str

    def get_db_connection_with_retry(self, username: Optional[str] = None,
                                    password: Optional[str] = None,
                                    max_retries: int = 3) -> str:
        """
        Get connection string with automatic retry logic.
        If primary fails, automatically tries standby.
        """
        for attempt in range(max_retries):
            try:
                conn_str = self.get_db_connection_string(username, password)
                return conn_str
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2)  # Wait before retry
                    # Force refresh to check for failover
                    self.manager.get_active_cluster(force_check=True)
                else:
                    raise

        raise Exception("Failed to get database connection after retries")
