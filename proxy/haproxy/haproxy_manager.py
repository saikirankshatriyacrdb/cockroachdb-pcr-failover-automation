#!/usr/bin/env python3
"""
HAProxy Manager - Manages HAProxy configuration for seamless failover
Updates HAProxy backend servers when failover occurs
"""
import subprocess
import logging
import os
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HAProxyManager:
    """Manages HAProxy configuration for CockroachDB PCR failover"""

    def __init__(self, haproxy_socket='/var/run/haproxy/admin.sock',
                 haproxy_cfg='/etc/haproxy/haproxy.cfg'):
        """
        Initialize HAProxy manager

        Args:
            haproxy_socket: HAProxy admin socket path
            haproxy_cfg: HAProxy configuration file path
        """
        self.haproxy_socket = haproxy_socket
        self.haproxy_cfg = haproxy_cfg
        self.primary_dns = os.getenv("PRIMARY_SQL_DNS", "<YOUR_PRIMARY_SQL_DNS>")
        self.standby_dns = os.getenv("STANDBY_SQL_DNS", "<YOUR_STANDBY_SQL_DNS>")

    def execute_haproxy_command(self, command: str) -> bool:
        """Execute a command via HAProxy admin socket"""
        try:
            cmd = f'echo "{command}" | socat stdio {self.haproxy_socket}'
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"HAProxy command executed: {command}")
                return True
            else:
                logger.error(f"HAProxy command failed: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"Error executing HAProxy command: {str(e)}")
            return False

    def set_server_state(self, server_name: str, backend: str, state: str):
        """
        Set server state in HAProxy

        Args:
            server_name: Server name in HAProxy config
            backend: Backend name
            state: State ('ready', 'drain', 'maint', 'disabled')
        """
        command = f"set server {backend}/{server_name} state {state}"
        return self.execute_haproxy_command(command)

    def switch_to_primary(self):
        """Switch HAProxy routing to primary cluster"""
        logger.info("Switching HAProxy routing to PRIMARY cluster...")

        self.set_server_state('primary', 'cockroach_backend', 'ready')
        self.set_server_state('standby', 'cockroach_backend', 'ready')

        self.execute_haproxy_command("set weight cockroach_backend/primary 100")
        self.execute_haproxy_command("set weight cockroach_backend/standby 10")

        logger.info("HAProxy routing switched to PRIMARY cluster")

    def switch_to_standby(self):
        """Switch HAProxy routing to standby cluster (failover)"""
        logger.info("Switching HAProxy routing to STANDBY cluster (failover)...")

        self.set_server_state('primary', 'cockroach_backend', 'maint')
        self.set_server_state('standby', 'cockroach_backend', 'ready')

        self.execute_haproxy_command("set weight cockroach_backend/standby 100")

        logger.info("HAProxy routing switched to STANDBY cluster")

    def reload_config(self):
        """Reload HAProxy configuration"""
        try:
            result = subprocess.run(['systemctl', 'reload', 'haproxy'],
                                   capture_output=True, text=True)
            if result.returncode == 0:
                logger.info("HAProxy configuration reloaded")
                return True
            else:
                logger.error(f"Failed to reload HAProxy: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"Error reloading HAProxy: {str(e)}")
            return False


class HAProxyFailoverHandler:
    """Handles HAProxy updates during failover events"""

    def __init__(self):
        self.haproxy = HAProxyManager()

    def on_failover_started(self, standby_dns: str = None):
        """Called when failover is initiated"""
        logger.info("Failover started - Updating HAProxy...")
        self.haproxy.switch_to_standby()

    def on_failover_completed(self, active_cluster_dns: str, is_primary: bool):
        """Called when failover completes"""
        logger.info(f"Failover completed - Active cluster: {active_cluster_dns}")
        if is_primary:
            self.haproxy.switch_to_primary()
        else:
            self.haproxy.switch_to_standby()

    def on_cluster_restored(self, primary_dns: str):
        """Called when primary cluster is restored"""
        logger.info(f"Primary cluster restored - Updating HAProxy...")
        self.haproxy.switch_to_primary()
