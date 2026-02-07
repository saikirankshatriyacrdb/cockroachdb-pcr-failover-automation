# CockroachDB PCR Automated Failover

Production-ready automation for CockroachDB Physical Cluster Replication (PCR) failover. Monitors primary cluster health and automatically fails over to the standby cluster when failures are detected.

## Features

- **Automated health monitoring** with configurable check intervals and failure thresholds
- **Automatic failover** to standby cluster when primary fails
- **Failback support** to restore traffic to the original primary
- **Connection management** with automatic routing to the active cluster
- **HTTP sidecar service** for applications to query connection strings
- **Proxy layer integration** (HAProxy, PgBouncer, Route 53)
- **Testing tools** for simulating cluster disruptions
- **Unified CLI** for all operations

## Quick Start

### 1. Configure

```bash
cp .env.example .env
# Edit .env with your CockroachDB Cloud API key and cluster IDs
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Verify connection

```bash
python cli.py test connection
```

### 4. Check status

```bash
python cli.py status
```

### 5. Start monitoring

```bash
python cli.py monitor
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `python cli.py monitor` | Continuous health monitoring with auto-failover |
| `python cli.py failover` | Manual failover trigger |
| `python cli.py failback <action>` | Failback operations (`full`, `create-stream`, `initiate`, `status`) |
| `python cli.py simulate <action>` | Disrupt/restore/status for testing |
| `python cli.py setup` | Create PCR stream |
| `python cli.py status` | Quick cluster & stream status |
| `python cli.py test <action>` | Run tests (`connection`, `monitoring`, `failover`, `automated`) |
| `python cli.py service` | HTTP sidecar service |
| `python cli.py settings` | Cluster settings management |
| `python cli.py debug` | Raw API response debugging |

Use `--help` on any command for details:

```bash
python cli.py --help
python cli.py failback --help
```

## Configuration

All configuration is via environment variables (`.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `COCKROACH_API_SECRET_KEY` | *required* | CockroachDB Cloud API secret key |
| `PRIMARY_CLUSTER_ID` | *required* | Primary cluster UUID |
| `STANDBY_CLUSTER_ID` | *required* | Standby cluster UUID |
| `PCR_STREAM_ID` | auto-discovered | PCR stream UUID |
| `HEALTH_CHECK_INTERVAL` | `30` | Seconds between health checks |
| `HEALTH_CHECK_TIMEOUT` | `10` | API request timeout in seconds |
| `FAILURE_THRESHOLD` | `3` | Consecutive failures before failover |
| `FAILOVER_DELAY` | `60` | Seconds to wait after threshold before failover |
| `FAILOVER_TO_LATEST` | `true` | Failover to latest consistent time |
| `FAILOVER_AT_TIMESTAMP` | — | Optional ISO timestamp for failover |
| `PRIMARY_CLUSTER_ENDPOINT` | — | Connection string for direct SQL access |
| `PRIMARY_SQL_DNS` | — | Primary cluster SQL DNS (for proxy layer) |
| `STANDBY_SQL_DNS` | — | Standby cluster SQL DNS (for proxy layer) |
| `ENABLE_PROMETHEUS` | `false` | Enable Prometheus metrics |
| `PROMETHEUS_PORT` | `8000` | Prometheus metrics port |

## How It Works

1. **Health monitoring**: The monitor checks primary cluster health via the CockroachDB Cloud API at configured intervals.

2. **Failure detection**: When a health check fails, the failure counter increments. After reaching `FAILURE_THRESHOLD` consecutive failures, the system waits `FAILOVER_DELAY` seconds.

3. **Double-check**: After the delay, the system re-checks primary health. If still unhealthy, it proceeds with failover.

4. **PCR stream validation**: Before failover, the system verifies the PCR stream is in `REPLICATING` or `STARTING` status.

5. **Failover execution**: The system patches the PCR stream status to `FAILING_OVER` via the API.

6. **Progress monitoring**: The system polls the stream status until it reaches `COMPLETED`.

### Timeline Example

With default settings (30s interval, 3 failures, 60s delay):

```
T+0:00   Primary goes down
T+0:30   Health check #1 fails (1/3)
T+1:00   Health check #2 fails (2/3)
T+1:30   Health check #3 fails (3/3) - threshold reached
T+1:30   Wait 60 seconds...
T+2:30   Double-check - still down, initiate failover
T+2:30+  Failover in progress (typically 1-5 minutes)
```

## Docker Deployment

### Monitor only

```bash
docker compose up -d failover-automation
```

### Monitor + HTTP service

```bash
docker compose --profile service up -d
```

### Build only

```bash
docker build -t pcr-automation .
docker run --env-file .env pcr-automation
```

## Application Integration

See `examples/app_integration.py` for integration patterns:

1. **psycopg2** with `ApplicationConnectionHelper`
2. **SQLAlchemy** with `pool_pre_ping`
3. **Multi-host connection strings** for driver-level failover
4. **Connection pool** with automatic health checks
5. **Environment variable setup** for legacy applications

### HTTP Service (Sidecar Pattern)

```bash
python cli.py service --port 8080
```

Endpoints:

- `GET /health` — Service health + active cluster ID
- `GET /connection` — Connection string (requires `X-DB-Username`, `X-DB-Password` headers)
- `GET /clusters` — Both clusters' health status and DNS

## Proxy Layer

See `proxy/README.md` for HAProxy and PgBouncer setup.

## Testing

```bash
# Verify API connectivity
python cli.py test connection

# Short monitoring test (3 cycles)
python cli.py test monitoring

# Full failover test (disrupts primary cluster)
python cli.py test failover

# Automated test with scheduled actions
python cli.py test automated
```

See `docs/testing.md` for detailed testing procedures.

## Failback

After a failover, to restore traffic to the original primary:

```bash
# Full automated failback
python cli.py failback full

# Or step-by-step:
python cli.py failback create-stream   # Create reverse PCR stream
python cli.py failback status          # Check replication status
python cli.py failback initiate        # Trigger failback
```

See `docs/failback.md` for details.

## Cluster Settings

Before testing, you may want to adjust `server.time_until_store_dead`:

```bash
# Show the SQL command
python cli.py settings --setting server.time_until_store_dead --value 15m0s

# Execute directly (requires PRIMARY_CLUSTER_ENDPOINT in .env and psycopg2)
python cli.py settings --setting server.time_until_store_dead --value 15m0s --execute
```

## Project Structure

```
pcr-automation-cl/
├── cli.py                   # Unified CLI entry point
├── src/
│   ├── config.py            # Configuration from environment variables
│   ├── cluster_monitor.py   # Health monitoring and failover logic
│   ├── failback_manager.py  # Failback operations
│   ├── connection_manager.py# Connection string management
│   ├── failover_service.py  # HTTP sidecar service
│   ├── pcr_setup.py         # PCR stream creation
│   └── simulate.py          # Cluster disruption simulation
├── tests/                   # Test scripts
├── proxy/                   # Proxy layer (HAProxy, PgBouncer, Route 53)
├── examples/                # Integration examples
├── docs/                    # Additional documentation
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Important Notes

- **Post-failover**: After automatic failover, redirect application traffic to the standby cluster. The automation handles cluster-level failover but not application routing (unless using the proxy layer or connection manager).
- **API key security**: Never commit your `.env` file. The `.gitignore` excludes it.
- **Non-production testing**: The `simulate` command uses the real CockroachDB Cloud disruption API. Only use in test environments.
- **Manual intervention**: After failover, create a new PCR stream for failback capability.
