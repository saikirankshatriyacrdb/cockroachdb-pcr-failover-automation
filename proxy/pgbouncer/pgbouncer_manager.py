#!/usr/bin/env python3
"""
PgBouncer Manager - Manages PgBouncer configuration for seamless failover
PgBouncer is a PostgreSQL connection pooler, compatible with CockroachDB
"""
import logging
import subprocess
import os
from typing import Optional, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PgBouncerManager:
    """Manages PgBouncer configuration for CockroachDB PCR failover"""

    def __init__(self, admin_host='127.0.0.1', admin_port=6432,
                 admin_user='admin', config_file='pgbouncer.ini'):
        """
        Initialize PgBouncer manager

        Args:
            admin_host: PgBouncer admin interface host
            admin_port: PgBouncer admin interface port (default: 6432)
            admin_user: PgBouncer admin user
            config_file: Path to pgbouncer.ini configuration file
        """
        self.admin_host = admin_host
        self.admin_port = admin_port
        self.admin_user = admin_user
        self.config_file = config_file

        self.primary_host = os.getenv("PRIMARY_SQL_DNS", "<YOUR_PRIMARY_SQL_DNS>")
        self.primary_port = 26257
        self.primary_db = os.getenv("PRIMARY_DB_NAME", "defaultdb")

        self.standby_host = os.getenv("STANDBY_SQL_DNS", "<YOUR_STANDBY_SQL_DNS>")
        self.standby_port = 26257
        self.standby_db = os.getenv("STANDBY_DB_NAME", "defaultdb")

    def connect_admin(self):
        """Connect to PgBouncer admin interface"""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.admin_host,
                port=self.admin_port,
                user=self.admin_user,
                database='pgbouncer'
            )
            logger.info(f"Connected to PgBouncer admin at {self.admin_host}:{self.admin_port}")
            return conn
        except ImportError:
            logger.error("psycopg2 required for PgBouncer admin. Install with: pip install psycopg2-binary")
            return None
        except Exception as e:
            logger.error(f"Failed to connect to PgBouncer admin: {str(e)}")
            return None

    def reload_config(self):
        """Reload PgBouncer configuration"""
        try:
            conn = self.connect_admin()
            if not conn:
                return False

            with conn.cursor() as cur:
                cur.execute("RELOAD")
                conn.commit()

            conn.close()
            logger.info("PgBouncer configuration reloaded")
            return True
        except Exception as e:
            logger.error(f"Failed to reload PgBouncer: {str(e)}")
            return False

    def update_config_file(self, use_primary: bool = True):
        """
        Update pgbouncer.ini configuration file

        Args:
            use_primary: If True, use primary as main database, else use standby
        """
        try:
            with open(self.config_file, 'r') as f:
                lines = f.readlines()

            new_lines = []
            in_databases_section = False

            for line in lines:
                if line.strip().startswith('[databases]'):
                    in_databases_section = True
                    new_lines.append(line)
                    if use_primary:
                        new_lines.append(f"cockroachdb = host={self.primary_host} port={self.primary_port} dbname={self.primary_db}\n")
                    else:
                        new_lines.append(f"cockroachdb = host={self.standby_host} port={self.standby_port} dbname={self.standby_db}\n")
                elif in_databases_section and line.strip().startswith('cockroachdb'):
                    continue
                elif in_databases_section and line.strip().startswith('['):
                    in_databases_section = False
                    new_lines.append(line)
                else:
                    new_lines.append(line)

            with open(self.config_file, 'w') as f:
                f.writelines(new_lines)

            logger.info(f"Updated {self.config_file} to use {'primary' if use_primary else 'standby'}")
            return True

        except Exception as e:
            logger.error(f"Failed to update config file: {str(e)}")
            return False

    def switch_to_primary(self):
        """Switch PgBouncer to use primary cluster"""
        logger.info("Switching PgBouncer to PRIMARY cluster...")
        if self.update_config_file(use_primary=True):
            self.reload_config()
            logger.info("PgBouncer switched to PRIMARY cluster")
            return True
        return False

    def switch_to_standby(self):
        """Switch PgBouncer to use standby cluster"""
        logger.info("Switching PgBouncer to STANDBY cluster (failover)...")
        if self.update_config_file(use_primary=False):
            self.reload_config()
            logger.info("PgBouncer switched to STANDBY cluster")
            return True
        return False

    def get_pools(self) -> Optional[list]:
        """Get current pool information"""
        conn = self.connect_admin()
        if not conn:
            return None

        try:
            with conn.cursor() as cur:
                cur.execute("SHOW POOLS")
                pools = cur.fetchall()
                return pools
        except Exception as e:
            logger.error(f"Failed to get pools: {str(e)}")
            return None
        finally:
            conn.close()


class PgBouncerFailoverHandler:
    """Handles PgBouncer updates during failover events"""

    def __init__(self, config_file='pgbouncer.ini'):
        self.pgbouncer = PgBouncerManager(config_file=config_file)

    def on_failover_started(self):
        """Called when failover is initiated"""
        logger.info("Failover started - Updating PgBouncer...")
        self.pgbouncer.switch_to_standby()

    def on_failover_completed(self, is_primary: bool):
        """Called when failover completes"""
        logger.info(f"Failover completed - Active cluster: {'primary' if is_primary else 'standby'}")
        if is_primary:
            self.pgbouncer.switch_to_primary()
        else:
            self.pgbouncer.switch_to_standby()

    def on_cluster_restored(self):
        """Called when primary cluster is restored"""
        logger.info("Primary cluster restored - Updating PgBouncer...")
        self.pgbouncer.switch_to_primary()
