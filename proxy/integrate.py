#!/usr/bin/env python3
"""
Integration script to connect proxy management with failover automation
Updates HAProxy or PgBouncer when failover events occur
"""
import sys
import os
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from proxy.haproxy.haproxy_manager import HAProxyFailoverHandler
    HAPROXY_AVAILABLE = True
except ImportError:
    HAPROXY_AVAILABLE = False

try:
    from proxy.pgbouncer.pgbouncer_manager import PgBouncerFailoverHandler
    PGBOUNCER_AVAILABLE = True
except ImportError:
    PGBOUNCER_AVAILABLE = False


def update_proxy_on_failover(standby_dns: str = None, proxy_type: str = 'haproxy'):
    """
    Update proxy routing when failover occurs

    Args:
        standby_dns: Standby cluster DNS (optional, uses env var STANDBY_SQL_DNS)
        proxy_type: Proxy type ('haproxy' or 'pgbouncer')
    """
    logger.info("=" * 60)
    logger.info(f"Updating {proxy_type.upper()} for Failover")
    logger.info("=" * 60)

    standby_dns = standby_dns or os.getenv("STANDBY_SQL_DNS", "<YOUR_STANDBY_SQL_DNS>")

    try:
        if proxy_type == 'haproxy' and HAPROXY_AVAILABLE:
            handler = HAProxyFailoverHandler()
            handler.on_failover_started(standby_dns)
            logger.info("HAProxy updated for failover")
        elif proxy_type == 'pgbouncer' and PGBOUNCER_AVAILABLE:
            handler = PgBouncerFailoverHandler()
            handler.on_failover_started()
            logger.info("PgBouncer updated for failover")
        else:
            logger.error(f"{proxy_type} not available or not configured")
    except Exception as e:
        logger.error(f"Failed to update {proxy_type}: {str(e)}")
        logger.error(f"   Make sure {proxy_type} is running and accessible")


def update_proxy_on_restore(primary_dns: str = None, proxy_type: str = 'haproxy'):
    """
    Update proxy routing when primary is restored

    Args:
        primary_dns: Primary cluster DNS (optional, uses env var PRIMARY_SQL_DNS)
        proxy_type: Proxy type ('haproxy' or 'pgbouncer')
    """
    logger.info("=" * 60)
    logger.info(f"Updating {proxy_type.upper()} for Primary Restoration")
    logger.info("=" * 60)

    primary_dns = primary_dns or os.getenv("PRIMARY_SQL_DNS", "<YOUR_PRIMARY_SQL_DNS>")

    try:
        if proxy_type == 'haproxy' and HAPROXY_AVAILABLE:
            handler = HAProxyFailoverHandler()
            handler.on_cluster_restored(primary_dns)
            logger.info("HAProxy updated for primary restoration")
        elif proxy_type == 'pgbouncer' and PGBOUNCER_AVAILABLE:
            handler = PgBouncerFailoverHandler()
            handler.on_cluster_restored()
            logger.info("PgBouncer updated for primary restoration")
        else:
            logger.error(f"{proxy_type} not available or not configured")
    except Exception as e:
        logger.error(f"Failed to update {proxy_type}: {str(e)}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Update proxy for failover events')
    parser.add_argument('action', choices=['failover', 'restore'],
                       help='Action to perform')
    parser.add_argument('--standby-dns', type=str,
                       help='Standby cluster DNS')
    parser.add_argument('--primary-dns', type=str,
                       help='Primary cluster DNS')
    parser.add_argument('--proxy', type=str,
                       choices=['haproxy', 'pgbouncer'],
                       default='haproxy',
                       help='Proxy type (default: haproxy)')

    args = parser.parse_args()

    if args.action == 'failover':
        update_proxy_on_failover(args.standby_dns, args.proxy)
    elif args.action == 'restore':
        update_proxy_on_restore(args.primary_dns, args.proxy)
