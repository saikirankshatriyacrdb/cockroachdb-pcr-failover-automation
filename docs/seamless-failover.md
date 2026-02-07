# Seamless Failover Guide

Three approaches for applications to handle CockroachDB PCR failover transparently.

## Option 1: ConnectionManager (Recommended)

The `ConnectionManager` class automatically routes connections to the active cluster.

```python
from src.connection_manager import ApplicationConnectionHelper

helper = ApplicationConnectionHelper()

# Automatically routes to the healthy cluster
conn_string = helper.get_db_connection_with_retry()

# Use with psycopg2
import psycopg2
conn = psycopg2.connect(conn_string)
```

**How it works:**
- Checks primary cluster health via the CockroachDB Cloud API
- Falls back to standby if primary is unhealthy
- Caches results for 60 seconds to avoid excessive API calls
- Supports retry with exponential backoff

**Requirements:**
- Set `DB_USERNAME` and `DB_PASSWORD` environment variables
- Or pass credentials directly: `helper.get_db_connection_string(username, password)`

## Option 2: Multi-Host Connection Strings

PostgreSQL drivers support multiple hosts in a connection string. The driver tries each host in order.

```python
from src.connection_manager import ConnectionManager

manager = ConnectionManager()
conn_str = manager.get_multi_host_connection_string("user", "pass")
# Returns: postgresql://user:pass@primary:26257,standby:26257/defaultdb?sslmode=require
```

**Supported drivers:**
- psycopg2 (Python)
- libpq (C)
- pgx (Go)
- node-postgres (Node.js)
- JDBC PostgreSQL (Java)

## Option 3: HTTP Service (Sidecar)

Run the failover service as a sidecar and query it for connection strings.

```bash
python cli.py service --port 8080
```

**Endpoints:**

```bash
# Health check
curl http://localhost:8080/health

# Get connection string
curl -H "X-DB-Username: user" -H "X-DB-Password: pass" \
     http://localhost:8080/connection

# Get cluster status
curl http://localhost:8080/clusters
```

**Response format:**

```json
{
  "connection_string": "postgresql://user:pass@host:26257/defaultdb?sslmode=require",
  "active_cluster": "<cluster-id>",
  "primary_cluster": "<primary-id>",
  "standby_cluster": "<standby-id>"
}
```

## Choosing an Approach

| Approach | Best For | Pros | Cons |
|----------|----------|------|------|
| ConnectionManager | Python apps | Direct, no extra services | Python only |
| Multi-host strings | Any language | Driver-native, no overhead | Requires compatible driver |
| HTTP service | Polyglot, microservices | Language-agnostic | Extra service to manage |
