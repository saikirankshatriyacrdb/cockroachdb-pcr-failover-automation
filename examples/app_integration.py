#!/usr/bin/env python3
"""
Example integrations for applications to use seamless failover
"""
import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.connection_manager import ApplicationConnectionHelper, ConnectionManager


# Example 1: Simple Python application using psycopg2
def example_psycopg2_integration():
    """Example: Using with psycopg2"""
    try:
        import psycopg2

        helper = ApplicationConnectionHelper()

        # Get connection string (automatically routes to active cluster)
        conn_string = helper.get_db_connection_with_retry()

        # Connect to database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        # Use database
        cursor.execute("SELECT version();")
        result = cursor.fetchone()
        print(f"Connected to: {result[0]}")

        cursor.close()
        conn.close()

    except ImportError:
        print("psycopg2 not installed: pip install psycopg2-binary")
    except Exception as e:
        print(f"Connection error: {str(e)}")


# Example 2: SQLAlchemy integration
def example_sqlalchemy_integration():
    """Example: Using with SQLAlchemy"""
    try:
        from sqlalchemy import create_engine

        helper = ApplicationConnectionHelper()
        conn_string = helper.get_db_connection_with_retry()

        # Create SQLAlchemy engine
        engine = create_engine(conn_string, pool_pre_ping=True)

        # Use engine
        with engine.connect() as conn:
            result = conn.execute("SELECT version();")
            print(f"Connected via SQLAlchemy: {result.fetchone()[0]}")

    except ImportError:
        print("SQLAlchemy not installed: pip install sqlalchemy")
    except Exception as e:
        print(f"Connection error: {str(e)}")


# Example 3: Multi-host connection string (driver-level failover)
def example_multi_host_connection():
    """Example: Using multi-host connection string for driver-level failover"""
    manager = ConnectionManager()
    username = os.getenv("DB_USERNAME", "<YOUR_USERNAME>")
    password = os.getenv("DB_PASSWORD", "<YOUR_PASSWORD>")

    # Get multi-host connection string
    multi_host_conn = manager.get_multi_host_connection_string(username, password)

    print(f"Multi-host connection string:")
    print(multi_host_conn)
    print("\nPostgreSQL drivers will automatically try hosts in order")
    print("If primary fails, driver will try standby automatically")


# Example 4: Application with connection pooling and health checks
class DatabaseConnectionPool:
    """Example connection pool with automatic failover"""

    def __init__(self):
        self.helper = ApplicationConnectionHelper()
        self.connections = {}
        self.health_check_interval = 30
        self.last_health_check = 0

    def get_connection(self):
        """Get a database connection, automatically fails over if needed"""
        current_time = time.time()
        if current_time - self.last_health_check > self.health_check_interval:
            self._refresh_connection_string()
            self.last_health_check = current_time

        conn_string = self.helper.get_db_connection_string()

        try:
            import psycopg2
            return psycopg2.connect(conn_string)
        except Exception as e:
            print(f"Connection failed, refreshing: {str(e)}")
            self._refresh_connection_string(force=True)
            conn_string = self.helper.get_db_connection_string()
            import psycopg2
            return psycopg2.connect(conn_string)

    def _refresh_connection_string(self, force=False):
        """Refresh connection string cache"""
        self.helper.manager.get_active_cluster(force_check=force)


# Example 5: Environment variable setup
def setup_environment_variables():
    """Set up environment variables for applications"""
    manager = ConnectionManager()

    active_cluster = manager.get_active_cluster()
    sql_dns = manager.get_cluster_sql_dns(active_cluster)

    os.environ["COCKROACH_HOST"] = sql_dns or ""
    os.environ["COCKROACH_PORT"] = "26257"
    os.environ["COCKROACH_ACTIVE_CLUSTER"] = active_cluster or ""

    print(f"Environment variables set:")
    print(f"  COCKROACH_HOST={sql_dns}")
    print(f"  COCKROACH_PORT=26257")
    print(f"  COCKROACH_ACTIVE_CLUSTER={active_cluster}")


if __name__ == "__main__":
    print("=" * 60)
    print("Application Integration Examples")
    print("=" * 60)

    print("\n1. Multi-host connection string:")
    example_multi_host_connection()

    print("\n2. Environment variable setup:")
    setup_environment_variables()

    print("\n3. For psycopg2 integration, see: example_psycopg2_integration()")
    print("4. For SQLAlchemy integration, see: example_sqlalchemy_integration()")
