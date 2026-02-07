#!/usr/bin/env python3
"""
CockroachDB PCR Automation CLI

Unified entry point for all PCR automation operations:
  monitor   - Continuous health monitoring with auto-failover
  failover  - Manual failover trigger
  failback  - Failback operations (full/create-stream/initiate/status)
  simulate  - Disrupt/restore/status for testing
  setup     - Create PCR stream
  status    - Quick cluster & stream status
  test      - Run tests (connection/monitoring/failover/automated)
  service   - HTTP sidecar service
  settings  - Cluster settings management
  debug     - Raw API response debugging
"""
import argparse
import sys
import time
import signal
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# monitor
# ---------------------------------------------------------------------------
def cmd_monitor(args):
    """Continuous health monitoring with auto-failover"""
    from src.cluster_monitor import ClusterMonitor
    from src.config import Config

    # Apply CLI overrides
    if args.interval is not None:
        Config.HEALTH_CHECK_INTERVAL = args.interval
    if args.threshold is not None:
        Config.FAILURE_THRESHOLD = args.threshold
    if args.delay is not None:
        Config.FAILOVER_DELAY = args.delay

    monitor = ClusterMonitor()
    running = True

    def signal_handler(signum, frame):
        nonlocal running
        logger.info("Received shutdown signal. Stopping automation...")
        running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting automated failover service...")
    logger.info(f"Health check interval: {Config.HEALTH_CHECK_INTERVAL} seconds")
    logger.info(f"Failure threshold: {Config.FAILURE_THRESHOLD} consecutive failures")
    logger.info(f"Failover delay: {Config.FAILOVER_DELAY} seconds")
    logger.info(f"Primary cluster ID: {Config.PRIMARY_CLUSTER_ID}")
    logger.info(f"Standby cluster ID: {Config.STANDBY_CLUSTER_ID}")

    while running:
        try:
            failover_initiated = monitor.detect_and_handle_failure()
            if failover_initiated:
                logger.critical("Failover has been initiated. Service will continue monitoring.")
            time.sleep(Config.HEALTH_CHECK_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            break
        except Exception as e:
            logger.error(f"Error in monitoring loop: {str(e)}")
            time.sleep(Config.HEALTH_CHECK_INTERVAL)

    logger.info("Automated failover service stopped.")


# ---------------------------------------------------------------------------
# failover
# ---------------------------------------------------------------------------
def cmd_failover(args):
    """Manual failover trigger"""
    from src.cluster_monitor import ClusterMonitor
    from src.config import Config

    monitor = ClusterMonitor()

    print("=" * 60)
    print("Manual Failover")
    print("=" * 60)
    print(f"\nPrimary Cluster: {Config.PRIMARY_CLUSTER_ID}")
    print(f"Standby Cluster: {Config.STANDBY_CLUSTER_ID}")

    # Check PCR stream
    stream_info = monitor.get_pcr_stream_info()
    if stream_info:
        print(f"PCR Stream Status: {stream_info.get('status', 'N/A')}")
    else:
        print("WARNING: Could not retrieve PCR stream info")

    if not args.yes:
        confirm = input("\nType 'YES' to initiate failover: ")
        if confirm != 'YES':
            print("Cancelled")
            return

    timestamp = getattr(args, 'failover_at', None)
    success = monitor.initiate_failover(failover_at=timestamp)

    if success:
        print("\nFailover initiated! Monitoring progress...")
        monitor.monitor_failover_progress()
    else:
        print("\nFailed to initiate failover")
        sys.exit(1)


# ---------------------------------------------------------------------------
# failback
# ---------------------------------------------------------------------------
def cmd_failback(args):
    """Failback operations"""
    from src.failback_manager import FailbackManager
    from src.config import Config

    manager = FailbackManager()

    print("=" * 60)
    print("CockroachDB Failback Manager")
    print("=" * 60)
    print(f"\nCurrent Active Cluster (was standby): {Config.STANDBY_CLUSTER_ID}")
    print(f"Original Primary Cluster (target): {Config.PRIMARY_CLUSTER_ID}")
    print("")

    if args.action == 'status':
        print("Checking failback PCR stream status...")
        stream_info = manager.get_failback_stream_info()
        if stream_info:
            print(f"\n  Stream ID: {stream_info.get('id', 'N/A')}")
            print(f"  Status: {stream_info.get('status', 'N/A')}")
            print(f"  Primary: {stream_info.get('primary_cluster_id', 'N/A')}")
            print(f"  Standby: {stream_info.get('standby_cluster_id', 'N/A')}")
            print(f"  Replicated Time: {stream_info.get('replicated_time', 'N/A')}")
            print(f"  Replication Lag: {stream_info.get('replication_lag_seconds', 'N/A')} seconds")
        else:
            print("\nNo failback PCR stream found")
            print("   Run: python cli.py failback create-stream")

    elif args.action == 'create-stream':
        print("Creating failback PCR stream...")
        stream_id = manager.create_failback_pcr_stream()
        if stream_id:
            print(f"\nFailback PCR stream created: {stream_id}")
            print("\nNext steps:")
            print("  1. Wait for stream to reach REPLICATING status")
            print("  2. Run: python cli.py failback initiate")
        else:
            print("\nFailed to create failback PCR stream")
            sys.exit(1)

    elif args.action == 'initiate':
        print("Initiating failback...")
        failover_at = getattr(args, 'failover_at', None)
        success = manager.initiate_failback(failover_at)
        if success:
            print("\nFailback initiated!")
            print("Monitoring progress...")
            manager.monitor_failback_progress()
        else:
            print("\nFailed to initiate failback")
            sys.exit(1)

    elif args.action == 'full':
        print("Executing full failback process...")
        print("This will:")
        print("  1. Verify both clusters are healthy")
        print("  2. Create failback PCR stream")
        wait = not getattr(args, 'no_wait', False)
        if wait:
            print("  3. Wait for replication to be ready")
        else:
            print("  3. Skip replication readiness check")
        print("  4. Initiate failback")
        print("  5. Monitor until completion")
        print("")

        if not args.yes:
            confirm = input("Continue? (yes/no): ")
            if confirm.lower() not in ['yes', 'y']:
                print("Cancelled")
                return

        success = manager.full_failback_process(wait_for_replication=wait)

        if success:
            print("\nFailback process completed successfully!")
            print("\nIMPORTANT: Redirect application traffic back to original primary cluster")
        else:
            print("\nFailback process failed")
            sys.exit(1)


# ---------------------------------------------------------------------------
# simulate
# ---------------------------------------------------------------------------
def cmd_simulate(args):
    """Simulate cluster disruption"""
    from src.simulate import run_simulate
    run_simulate(
        action=args.action,
        cluster_id=getattr(args, 'cluster_id', None),
        disruption_file=getattr(args, 'disruption_file', None),
        skip_confirm=getattr(args, 'yes', False)
    )


# ---------------------------------------------------------------------------
# setup
# ---------------------------------------------------------------------------
def cmd_setup(args):
    """Create PCR stream"""
    from src.pcr_setup import run_setup
    run_setup(
        api_key=getattr(args, 'api_key', None),
        primary=getattr(args, 'primary', None),
        secondary=getattr(args, 'secondary', None),
        no_monitor=getattr(args, 'no_monitor', False),
        max_wait=getattr(args, 'max_wait', 300),
        skip_confirm=getattr(args, 'yes', False)
    )


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
def cmd_status(args):
    """Quick cluster & stream status"""
    from src.cluster_monitor import ClusterMonitor
    from src.connection_manager import ConnectionManager
    from src.config import Config

    print("=" * 60)
    print("CockroachDB PCR Status")
    print("=" * 60)

    # Check configuration
    print("\nConfiguration:")
    print(f"  API Key: {'configured' if Config.API_SECRET_KEY else 'NOT SET'}")
    print(f"  Primary Cluster: {Config.PRIMARY_CLUSTER_ID or 'NOT SET'}")
    print(f"  Standby Cluster: {Config.STANDBY_CLUSTER_ID or 'NOT SET'}")
    print(f"  PCR Stream ID: {Config.PCR_STREAM_ID or 'will be discovered'}")

    if not Config.API_SECRET_KEY or not Config.PRIMARY_CLUSTER_ID:
        print("\nERROR: Missing required configuration. Check .env file.")
        return

    # Check cluster health
    monitor = ClusterMonitor()
    conn_mgr = ConnectionManager()

    print("\nCluster Health:")
    primary_healthy = conn_mgr.check_cluster_health(Config.PRIMARY_CLUSTER_ID)
    print(f"  Primary:  {'HEALTHY' if primary_healthy else 'UNHEALTHY'}")

    if Config.STANDBY_CLUSTER_ID:
        standby_healthy = conn_mgr.check_cluster_health(Config.STANDBY_CLUSTER_ID)
        print(f"  Standby:  {'HEALTHY' if standby_healthy else 'UNHEALTHY'}")

    # Check PCR stream
    print("\nPCR Stream:")
    stream_info = monitor.get_pcr_stream_info()
    if stream_info:
        print(f"  Stream ID: {stream_info.get('id', 'N/A')}")
        print(f"  Status: {stream_info.get('status', 'N/A')}")
        print(f"  Primary: {stream_info.get('primary_cluster_id', 'N/A')}")
        print(f"  Standby: {stream_info.get('standby_cluster_id', 'N/A')}")
        print(f"  Replicated Time: {stream_info.get('replicated_time', 'N/A')}")
        print(f"  Replication Lag: {stream_info.get('replication_lag_seconds', 'N/A')} seconds")
    else:
        print("  No PCR stream found or stream ID not discovered")

    # Active cluster
    active = conn_mgr.get_active_cluster()
    print(f"\nActive Cluster: {active}")
    sql_dns = conn_mgr.get_cluster_sql_dns(active)
    if sql_dns:
        print(f"SQL DNS: {sql_dns}")


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------
def cmd_test(args):
    """Run tests"""
    if args.action == 'connection':
        _test_connection()
    elif args.action == 'monitoring':
        _test_monitoring()
    elif args.action == 'failover':
        _test_failover()
    elif args.action == 'automated':
        _test_automated()


def _test_connection():
    """Test API connection and cluster status"""
    from src.cluster_monitor import ClusterMonitor
    from src.config import Config

    print("=" * 60)
    print("Testing CockroachDB Cloud API Connection")
    print("=" * 60)

    print("\n1. Checking Configuration...")
    if not Config.API_SECRET_KEY:
        print("ERROR: API_SECRET_KEY not set in .env file")
        return
    print(f"  API Key configured (length: {len(Config.API_SECRET_KEY)} chars)")

    if not Config.PRIMARY_CLUSTER_ID:
        print("ERROR: PRIMARY_CLUSTER_ID not set")
        return
    print(f"  Primary Cluster ID: {Config.PRIMARY_CLUSTER_ID}")

    if not Config.STANDBY_CLUSTER_ID:
        print("ERROR: STANDBY_CLUSTER_ID not set")
        return
    print(f"  Standby Cluster ID: {Config.STANDBY_CLUSTER_ID}")

    print("\n2. Initializing ClusterMonitor...")
    monitor = ClusterMonitor()
    print("  ClusterMonitor initialized")

    print("\n3. Checking Primary Cluster Health...")
    try:
        is_healthy = monitor.check_primary_cluster_health()
        if is_healthy:
            print("  Primary cluster is HEALTHY")
        else:
            print("  Primary cluster is UNHEALTHY or unreachable")
    except Exception as e:
        print(f"  Error checking primary cluster: {str(e)}")
        return

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


def _test_monitoring():
    """Short monitoring test"""
    from src.cluster_monitor import ClusterMonitor
    from src.config import Config

    print("=" * 60)
    print("Testing Automated Failover Monitoring")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  - Health Check Interval: {Config.HEALTH_CHECK_INTERVAL} seconds")
    print(f"  - Failure Threshold: {Config.FAILURE_THRESHOLD} consecutive failures")
    print(f"  - Primary Cluster: {Config.PRIMARY_CLUSTER_ID}")
    print(f"  - Standby Cluster: {Config.STANDBY_CLUSTER_ID}")
    print(f"  - PCR Stream ID: {Config.PCR_STREAM_ID or 'Will be discovered'}")

    print("\n" + "=" * 60)
    print("Starting monitoring (will run for 3 health check cycles)")
    print("Press Ctrl+C to stop early")
    print("=" * 60 + "\n")

    monitor = ClusterMonitor()
    cycles = 0
    max_cycles = 3

    try:
        while cycles < max_cycles:
            cycles += 1
            print(f"\n--- Health Check Cycle {cycles}/{max_cycles} ---")

            is_healthy = monitor.check_primary_cluster_health()
            if is_healthy:
                print("  Primary cluster is healthy")
                print(f"  Failure count reset to: {monitor.failure_count}")
            else:
                print("  Primary cluster health check failed")
                print(f"  Failure count: {monitor.failure_count}/{Config.FAILURE_THRESHOLD}")

            stream_info = monitor.get_pcr_stream_info()
            if stream_info:
                print(f"  PCR Stream Status: {stream_info.get('status', 'UNKNOWN')}")
                print(f"  Replicated Time: {stream_info.get('replicated_time', 'N/A')}")
                print(f"  Replication Lag: {stream_info.get('replication_lag_seconds', 'N/A')} seconds")

            if cycles < max_cycles:
                print(f"\nWaiting {Config.HEALTH_CHECK_INTERVAL} seconds until next check...")
                time.sleep(Config.HEALTH_CHECK_INTERVAL)

        print("\n" + "=" * 60)
        print("Test Complete! The monitoring system is working correctly.")
        print("=" * 60)
        print("\nTo run full automation: python cli.py monitor")

    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")
    except Exception as e:
        print(f"\nError during monitoring: {str(e)}")
        import traceback
        traceback.print_exc()


def _test_failover():
    """Complete failover test scenario"""
    from src.cluster_monitor import ClusterMonitor
    from src.config import Config
    from src.simulate import disrupt_cluster, restore_cluster, get_cluster_info

    print("=" * 60)
    print("CockroachDB Automated Failover Test Scenario")
    print("=" * 60)

    print("\nWARNING: This will disrupt your PRIMARY cluster!")
    print("   1. Disrupt the primary cluster")
    print("   2. Monitor for automatic failover")
    print("   3. Restore the primary cluster")
    print("\n   Make sure you're testing in a non-production environment!")

    confirm = input("\n   Type 'TEST' to proceed: ")
    if confirm != 'TEST':
        print("Test cancelled")
        return

    monitor = ClusterMonitor()

    # Step 1: Pre-test status
    print("\n" + "=" * 60)
    print("Step 1: Pre-Test Status Check")
    print("=" * 60)
    cluster_info = get_cluster_info(Config.PRIMARY_CLUSTER_ID)
    if cluster_info:
        print(f"   Cluster: {cluster_info.get('name', 'N/A')}")
        print(f"   State: {cluster_info.get('state', 'N/A')}")
        print(f"   Operation Status: {cluster_info.get('operation_status', 'N/A')}")

    stream_info = monitor.get_pcr_stream_info()
    if stream_info:
        print(f"   Stream ID: {stream_info.get('id', 'N/A')}")
        print(f"   Status: {stream_info.get('status', 'N/A')}")
        print(f"   Replication Lag: {stream_info.get('replication_lag_seconds', 'N/A')} seconds")

    # Step 2: Trigger disruption
    print("\n" + "=" * 60)
    print("Step 2: Triggering Cluster Disruption")
    print("=" * 60)
    disruption_success = disrupt_cluster(Config.PRIMARY_CLUSTER_ID)

    if not disruption_success:
        print("Failed to trigger disruption. Aborting test.")
        return

    # Step 3: Monitor
    print("\n" + "=" * 60)
    print("Step 3: Monitoring for Failover Trigger")
    print("=" * 60)
    print(f"   Failure threshold: {Config.FAILURE_THRESHOLD} consecutive failures")
    print(f"   Health check interval: {Config.HEALTH_CHECK_INTERVAL} seconds")
    print(f"   Failover delay: {Config.FAILOVER_DELAY} seconds")

    max_wait = (Config.FAILURE_THRESHOLD * Config.HEALTH_CHECK_INTERVAL) + Config.FAILOVER_DELAY + 60
    start_time = time.time()
    check_count = 0

    try:
        while time.time() - start_time < max_wait:
            check_count += 1
            elapsed = int(time.time() - start_time)
            print(f"\n--- Health Check #{check_count} (Elapsed: {elapsed}s) ---")

            is_healthy = monitor.check_primary_cluster_health()
            print(f"  {'Healthy' if is_healthy else 'FAILED'} | Failures: {monitor.failure_count}/{Config.FAILURE_THRESHOLD}")

            stream_info = monitor.get_pcr_stream_info()
            if stream_info:
                status = stream_info.get("status", "UNKNOWN")
                print(f"  PCR Stream Status: {status}")

                if status == "FAILING_OVER":
                    print("\nFAILOVER DETECTED!")
                    while time.time() - start_time < max_wait:
                        time.sleep(10)
                        stream_info = monitor.get_pcr_stream_info()
                        if stream_info:
                            status = stream_info.get("status", "UNKNOWN")
                            print(f"   Status: {status}")
                            if status == "COMPLETED":
                                print(f"\nFAILOVER COMPLETED at {stream_info.get('activated_at', 'N/A')}!")
                                break
                    break
                elif status == "COMPLETED":
                    print("\nFAILOVER ALREADY COMPLETED!")
                    break

            time.sleep(Config.HEALTH_CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\n\nMonitoring interrupted by user")

    # Step 4: Restore
    print("\n" + "=" * 60)
    print("Step 4: Restoring Primary Cluster")
    print("=" * 60)

    restore_confirm = input("   Restore cluster now? (yes/no): ")
    if restore_confirm.lower() in ['yes', 'y']:
        restore_cluster(Config.PRIMARY_CLUSTER_ID)
        print("\nWaiting 10 seconds for cluster to recover...")
        time.sleep(10)
        cluster_info = get_cluster_info(Config.PRIMARY_CLUSTER_ID)
        if cluster_info:
            print(f"   State: {cluster_info.get('state', 'N/A')}")
            print(f"   Operation Status: {cluster_info.get('operation_status', 'N/A')}")
    else:
        print("Skipping restoration. Please restore manually when ready.")

    print("\n" + "=" * 60)
    print("Test Scenario Complete")
    print("=" * 60)


def _test_automated():
    """Automated test scenario with scheduled actions"""
    from src.cluster_monitor import ClusterMonitor
    from src.config import Config
    from src.simulate import disrupt_cluster, get_cluster_info

    class AutomatedTest:
        def __init__(self):
            self.monitor = ClusterMonitor()
            self.running = True
            self.start_time = None
            self.disruption_triggered = False
            self.failover_triggered = False

        def get_replication_info(self):
            stream_info = self.monitor.get_pcr_stream_info()
            if stream_info:
                return {
                    "status": stream_info.get("status", "UNKNOWN"),
                    "replicated_time": stream_info.get("replicated_time", "N/A"),
                    "replication_lag_seconds": stream_info.get("replication_lag_seconds", 0),
                    "retained_time": stream_info.get("retained_time", "N/A")
                }
            return None

        def run(self):
            logger.info("=" * 60)
            logger.info("Automated Test Scenario")
            logger.info("=" * 60)
            logger.info(f"  Primary Cluster: {Config.PRIMARY_CLUSTER_ID}")
            logger.info(f"  Standby Cluster: {Config.STANDBY_CLUSTER_ID}")
            logger.info(f"  Health Check Interval: {Config.HEALTH_CHECK_INTERVAL} seconds")
            logger.info(f"\nTest Timeline:")
            logger.info(f"  T+0:00   - Start continuous monitoring")
            logger.info(f"  T+2:00   - Trigger disruption on primary")
            logger.info(f"  T+4:00   - Check replication lag and trigger failover")
            logger.info(f"\nPress Ctrl+C to stop early")
            logger.info("=" * 60 + "\n")

            self.start_time = time.time()
            try:
                check_count = 0
                while self.running:
                    check_count += 1
                    elapsed = time.time() - self.start_time

                    is_healthy = self.monitor.check_primary_cluster_health()
                    rep_info = self.get_replication_info()

                    status_line = f"[{elapsed:6.1f}s] Check #{check_count:3d} | "
                    status_line += f"Primary: {'OK' if is_healthy else 'FAIL'} | "
                    if rep_info:
                        status_line += f"PCR: {rep_info['status']:12s} | "
                        status_line += f"Lag: {rep_info['replication_lag_seconds']:3d}s"
                    else:
                        status_line += "PCR: N/A"
                    logger.info(status_line)

                    if elapsed >= 120 and not self.disruption_triggered:
                        logger.info("Triggering disruption...")
                        success = disrupt_cluster(Config.PRIMARY_CLUSTER_ID)
                        if success:
                            self.disruption_triggered = True

                    if elapsed >= 240 and not self.failover_triggered:
                        logger.info("Triggering failover...")
                        success = self.monitor.initiate_failover()
                        if success:
                            self.failover_triggered = True
                            self.monitor.monitor_failover_progress()

                    time.sleep(Config.HEALTH_CHECK_INTERVAL)

            except KeyboardInterrupt:
                logger.info("\nTest interrupted by user")
            finally:
                elapsed = time.time() - self.start_time
                logger.info(f"\nTotal duration: {elapsed:.1f} seconds")
                logger.info(f"Disruption triggered: {'yes' if self.disruption_triggered else 'no'}")
                logger.info(f"Failover triggered: {'yes' if self.failover_triggered else 'no'}")

    test = AutomatedTest()

    def sig_handler(signum, frame):
        test.running = False

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    test.run()


# ---------------------------------------------------------------------------
# service
# ---------------------------------------------------------------------------
def cmd_service(args):
    """HTTP sidecar service"""
    from src.failover_service import SeamlessFailoverService
    service = SeamlessFailoverService(port=args.port)
    service.run()


# ---------------------------------------------------------------------------
# settings
# ---------------------------------------------------------------------------
def cmd_settings(args):
    """Cluster settings management"""
    from src.config import Config
    import requests

    cluster_id = getattr(args, 'cluster_id', None) or Config.PRIMARY_CLUSTER_ID

    print("=" * 60)
    print("CockroachDB Cluster Settings Configuration")
    print("=" * 60)

    # Get cluster info
    url = f"{Config.API_BASE_URL}/clusters/{cluster_id}"
    headers = {
        "Authorization": f"Bearer {Config.API_SECRET_KEY}",
        "content-type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            cluster_info = response.json()
            print(f"\nCluster Information:")
            print(f"   Name: {cluster_info.get('name', 'N/A')}")
            print(f"   SQL DNS: {cluster_info.get('sql_dns', 'N/A')}")
            print(f"   State: {cluster_info.get('state', 'N/A')}")
        else:
            print(f"Failed to get cluster information: {response.status_code}")
            return
    except Exception as e:
        print(f"Error: {str(e)}")
        return

    setting = getattr(args, 'setting', 'server.time_until_store_dead')
    value = getattr(args, 'value', '15m0s')

    print(f"\nSetting: {setting} = {value}")
    print(f"   Cluster: {cluster_id}")

    if getattr(args, 'execute', False):
        connection_string = Config.PRIMARY_CLUSTER_ENDPOINT
        if not connection_string:
            print("\nNo connection string configured in .env")
            print("   Set PRIMARY_CLUSTER_ENDPOINT in .env to enable direct execution")
            print("   Format: postgresql://user:password@host:port/database")
        else:
            try:
                import psycopg2
                from urllib.parse import urlparse

                parsed = urlparse(connection_string)
                print(f"\nConnecting to database...")
                conn = psycopg2.connect(
                    host=parsed.hostname,
                    port=parsed.port or 26257,
                    user=parsed.username,
                    password=parsed.password,
                    database=parsed.path[1:] if parsed.path else 'defaultdb',
                    sslmode='require'
                )

                cur = conn.cursor()
                sql_command = f"SET CLUSTER SETTING {setting} = '{value}';"
                print(f"\nExecuting: {sql_command}")
                cur.execute(sql_command)
                conn.commit()
                print("Cluster setting updated successfully!")

                verify_sql = f"SHOW CLUSTER SETTING {setting};"
                cur.execute(verify_sql)
                result = cur.fetchone()
                if result:
                    print(f"\nVerified: {setting} = {result[0]}")

                cur.close()
                conn.close()
                return

            except ImportError:
                print("\npsycopg2 not installed. Install with: pip install psycopg2-binary")
            except Exception as e:
                print(f"\nError executing SQL: {str(e)}")

    # Show the SQL command to run manually
    print("\n" + "=" * 60)
    print("SQL Command to Execute:")
    print("=" * 60)
    print(f"\nSET CLUSTER SETTING {setting} = '{value}';")
    print("\n" + "=" * 60)
    print(f"\nTo execute directly, use --execute with PRIMARY_CLUSTER_ENDPOINT in .env")
    sql_dns = cluster_info.get('sql_dns', '<YOUR_SQL_DNS>') if 'cluster_info' in dir() else '<YOUR_SQL_DNS>'
    print(f"Or connect manually:")
    print(f"   cockroach sql --url='postgresql://<YOUR_USERNAME>:<PASSWORD>@{sql_dns}:26257/defaultdb?sslmode=require'")


# ---------------------------------------------------------------------------
# debug
# ---------------------------------------------------------------------------
def cmd_debug(args):
    """Raw API response debugging"""
    from src.config import Config
    import requests
    import json

    headers = {
        "Authorization": f"Bearer {Config.API_SECRET_KEY}",
        "content-type": "application/json"
    }

    print("=" * 60)
    print("Debugging CockroachDB Cloud API")
    print("=" * 60)

    # Test primary cluster
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
        else:
            print(f"\nError response:")
            print(response.text)
    except Exception as e:
        print(f"Error: {str(e)}")

    # Test standby cluster
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

    # Test PCR streams with primary
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
        else:
            print(f"\nError response:")
            print(response.text)
    except Exception as e:
        print(f"Error: {str(e)}")

    # Test PCR streams with standby
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


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------
def build_parser():
    parser = argparse.ArgumentParser(
        prog='cli.py',
        description='CockroachDB PCR Automation CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py status                    # Quick cluster & stream status
  python cli.py monitor                   # Start continuous monitoring
  python cli.py failover --yes            # Manual failover (skip confirmation)
  python cli.py failback full             # Full failback process
  python cli.py simulate disrupt          # Disrupt primary cluster
  python cli.py simulate restore          # Restore primary cluster
  python cli.py setup                     # Create PCR stream
  python cli.py test connection           # Test API connection
  python cli.py service --port 8080       # Start HTTP sidecar service
  python cli.py settings --setting server.time_until_store_dead --value 15m0s
  python cli.py debug                     # Debug raw API responses
"""
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # monitor
    p_monitor = subparsers.add_parser('monitor', help='Continuous health monitoring with auto-failover')
    p_monitor.add_argument('--interval', type=int, default=None,
                          help='Health check interval in seconds (default: from .env or 30)')
    p_monitor.add_argument('--threshold', type=int, default=None,
                          help='Consecutive failures before failover (default: from .env or 3)')
    p_monitor.add_argument('--delay', type=int, default=None,
                          help='Seconds to wait after threshold before failover (default: from .env or 60)')

    # failover
    p_failover = subparsers.add_parser('failover', help='Manual failover trigger')
    p_failover.add_argument('--yes', '-y', action='store_true', help='Skip confirmation')
    p_failover.add_argument('--failover-at', type=str, help='ISO timestamp for failover')

    # failback
    p_failback = subparsers.add_parser('failback', help='Failback operations')
    p_failback.add_argument('action', choices=['full', 'create-stream', 'initiate', 'status'],
                           help='Failback action')
    p_failback.add_argument('--yes', '-y', action='store_true', help='Skip confirmation')
    p_failback.add_argument('--no-wait', action='store_true', help='Skip replication readiness check')
    p_failback.add_argument('--failover-at', type=str, help='ISO timestamp for failover')

    # simulate
    p_simulate = subparsers.add_parser('simulate', help='Simulate cluster disruption')
    p_simulate.add_argument('action', choices=['disrupt', 'restore', 'status'],
                           help='Simulation action')
    p_simulate.add_argument('--cluster-id', type=str, help='Cluster ID (default: primary)')
    p_simulate.add_argument('--disruption-file', type=str, help='Path to disruption JSON file')
    p_simulate.add_argument('--yes', '-y', action='store_true', help='Skip confirmation')

    # setup
    p_setup = subparsers.add_parser('setup', help='Create PCR stream')
    p_setup.add_argument('--api-key', type=str, help='API secret key (default: from .env)')
    p_setup.add_argument('--primary', type=str, help='Primary cluster ID (default: from .env)')
    p_setup.add_argument('--secondary', '--standby', type=str, help='Secondary cluster ID (default: from .env)')
    p_setup.add_argument('--no-monitor', action='store_true', help='Skip monitoring stream status')
    p_setup.add_argument('--max-wait', type=int, default=300, help='Max wait for REPLICATING (default: 300s)')
    p_setup.add_argument('--yes', '-y', action='store_true', help='Skip confirmation')

    # status
    p_status = subparsers.add_parser('status', help='Quick cluster & stream status')

    # test
    p_test = subparsers.add_parser('test', help='Run tests')
    p_test.add_argument('action', choices=['connection', 'monitoring', 'failover', 'automated'],
                       help='Test type')

    # service
    p_service = subparsers.add_parser('service', help='HTTP sidecar service')
    p_service.add_argument('--port', type=int, default=8080, help='Port to listen on (default: 8080)')

    # settings
    p_settings = subparsers.add_parser('settings', help='Cluster settings management')
    p_settings.add_argument('--cluster-id', type=str, help='Cluster ID (default: primary)')
    p_settings.add_argument('--setting', default='server.time_until_store_dead', help='Setting name')
    p_settings.add_argument('--value', default='15m0s', help='Setting value')
    p_settings.add_argument('--execute', action='store_true', help='Execute via SQL connection')

    # debug
    p_debug = subparsers.add_parser('debug', help='Raw API response debugging')

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    commands = {
        'monitor': cmd_monitor,
        'failover': cmd_failover,
        'failback': cmd_failback,
        'simulate': cmd_simulate,
        'setup': cmd_setup,
        'status': cmd_status,
        'test': cmd_test,
        'service': cmd_service,
        'settings': cmd_settings,
        'debug': cmd_debug,
    }

    cmd_func = commands.get(args.command)
    if cmd_func:
        try:
            cmd_func(args)
        except KeyboardInterrupt:
            print("\n\nInterrupted by user")
            sys.exit(1)
        except Exception as e:
            print(f"\nError: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
