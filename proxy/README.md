# Proxy Layer for CockroachDB PCR Failover

This directory contains proxy configurations for transparent application failover. Applications connect to the proxy (on localhost), which routes traffic to the active CockroachDB cluster.

## Architecture

```
Application
    |
    v
Proxy (localhost:26257 or :6432)
    |
    +---> Primary Cluster (weight: 100)
    |
    +---> Standby Cluster (weight: 10, backup)
```

During failover, the proxy switches routing from primary to standby. Applications don't need to change connection strings.

## Options

### HAProxy (Recommended)

TCP-level proxy. Best for CockroachDB because it works at the TCP layer and supports the PostgreSQL wire protocol natively.

**Setup:**

1. Edit `haproxy/haproxy.cfg` — replace `<YOUR_PRIMARY_SQL_DNS>` and `<YOUR_STANDBY_SQL_DNS>` with your cluster DNS endpoints.

2. Start HAProxy:
   ```bash
   cd haproxy
   docker compose up -d
   ```

3. Verify stats at `http://localhost:8404/stats`

4. Connect applications to `localhost:26257`

**Failover integration:**

```bash
# When failover occurs, update HAProxy routing:
python integrate.py failover --proxy haproxy

# When primary is restored:
python integrate.py restore --proxy haproxy
```

### PgBouncer

PostgreSQL connection pooler. Adds connection pooling on top of failover routing.

**Setup:**

1. Edit `pgbouncer/pgbouncer.ini` — replace `<YOUR_PRIMARY_SQL_DNS>` with your cluster DNS.

2. Start PgBouncer:
   ```bash
   cd pgbouncer
   docker compose up -d
   ```

3. Connect applications to `localhost:6432`

**Failover integration:**

```bash
python integrate.py failover --proxy pgbouncer
python integrate.py restore --proxy pgbouncer
```

### Route 53

DNS-level failover using AWS Route 53. Creates health checks and failover DNS records.

**Setup:**

Requires AWS credentials and a Route 53 hosted zone.

```python
from proxy.route53.route53_setup import Route53FailoverManager

manager = Route53FailoverManager(
    hosted_zone_id="YOUR_HOSTED_ZONE_ID",
    domain_name="db.example.com"
)
manager.setup_failover_dns()
```

Applications then connect to `db.example.com:26257`.

## Integration with Automation

The `integrate.py` script bridges the failover automation with the proxy layer. It can be called automatically when failover events are detected, or manually:

```bash
# Update proxy when failover occurs
python integrate.py failover --proxy haproxy --standby-dns <dns>

# Update proxy when primary is restored
python integrate.py restore --proxy haproxy --primary-dns <dns>
```

## Why Localhost?

When using a proxy, applications connect to `localhost:26257` (HAProxy) or `localhost:6432` (PgBouncer). This provides:

1. **Transparency** — Applications don't need to know about failover
2. **No connection string changes** — Same connection string before and after failover
3. **Connection pooling** — PgBouncer provides built-in connection pooling
4. **Stats & monitoring** — HAProxy provides a stats interface at `:8404/stats`
