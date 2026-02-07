# Testing Guide

## Prerequisites

1. Configure `.env` with your API key and cluster IDs
2. Install dependencies: `pip install -r requirements.txt`
3. Verify connectivity: `python cli.py test connection`

## Test Levels

### 1. Connection Test

Validates configuration and API connectivity. Non-destructive.

```bash
python cli.py test connection
```

Checks:
- API key is configured
- Cluster IDs are set
- Primary cluster health check works
- PCR stream discovery works

### 2. Monitoring Test

Runs 3 health check cycles. Non-destructive.

```bash
python cli.py test monitoring
```

Verifies:
- Health check loop works correctly
- PCR stream status is readable
- Failure counter logic works

### 3. Failover Test

**WARNING: Disrupts the primary cluster.** Only use in test environments.

```bash
python cli.py test failover
```

Process:
1. Pre-test status check
2. Triggers disruption on primary cluster (requires confirmation)
3. Monitors health checks and failure counter
4. Waits for automatic failover
5. Offers to restore the cluster

### 4. Automated Test

**WARNING: Disrupts the primary cluster.** Runs on a schedule.

```bash
python cli.py test automated
```

Timeline:
- T+0:00 — Start continuous monitoring
- T+2:00 — Auto-trigger disruption on primary
- T+4:00 — Check replication lag and trigger failover

Press Ctrl+C to stop early.

## Manual Disruption Testing

### Disrupt (trigger failure)

```bash
# With confirmation prompt
python cli.py simulate disrupt

# Skip confirmation
python cli.py simulate disrupt --yes

# Custom disruption config
python cli.py simulate disrupt --disruption-file path/to/disruption.json

# Disrupt a specific cluster
python cli.py simulate disrupt --cluster-id <cluster-id>
```

### Restore

```bash
python cli.py simulate restore
```

### Check status

```bash
python cli.py simulate status
```

## Disruption Configuration

The disruption config file (`examples/disruption.json`) specifies which regions to disrupt:

```json
{
    "regional_disruptor_specifications": [
        {
            "region_code": "us-west-2",
            "is_whole_region": true
        }
    ]
}
```

Modify `region_code` to match your primary cluster's region.

## Expected Behavior

With default settings (30s interval, 3 failures, 60s delay):

| Time | Event |
|------|-------|
| T+0:00 | Primary goes down |
| T+0:30 | First health check fails (1/3) |
| T+1:00 | Second health check fails (2/3) |
| T+1:30 | Third health check fails (3/3) — threshold reached |
| T+1:30 | Failover delay starts (60 seconds) |
| T+2:30 | Double-check — primary still down |
| T+2:30 | PCR stream status validated |
| T+2:30 | Failover initiated (stream → FAILING_OVER) |
| T+2:30+ | Failover in progress |
| T+~5:00 | Failover completed (stream → COMPLETED) |

## Debugging

```bash
# See raw API responses
python cli.py debug

# Check cluster status
python cli.py simulate status

# Check PCR stream
python cli.py status
```

## Safety Notes

- The disruption API affects **real cloud infrastructure**. Only disrupt test clusters.
- After a failover test, the PCR stream is consumed. Create a new one for the next test: `python cli.py setup`
- Always restore the cluster after testing: `python cli.py simulate restore`
