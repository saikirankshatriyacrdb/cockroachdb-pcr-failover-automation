# Failback Guide

After a failover, use failback to restore traffic to the original primary cluster.

## Prerequisites

- Original primary cluster has been restored and is healthy
- Current active cluster (was standby) is healthy and serving traffic
- CockroachDB Cloud API credentials configured

## Quick Failback

```bash
python cli.py failback full
```

This runs the complete process automatically:
1. Verifies both clusters are healthy
2. Creates a reverse PCR stream (standby -> original primary)
3. Waits for replication to reach `REPLICATING` status
4. Initiates failback
5. Monitors until completion

## Step-by-Step Failback

### Step 1: Verify cluster health

```bash
python cli.py status
```

Both clusters should show `HEALTHY`.

### Step 2: Create failback PCR stream

```bash
python cli.py failback create-stream
```

This creates a new PCR stream in the reverse direction:
- **Primary**: Current active cluster (was standby)
- **Standby**: Original primary cluster (restored)

### Step 3: Wait for replication

```bash
python cli.py failback status
```

Wait until the stream status is `REPLICATING`. This means the original primary has caught up with the current active cluster.

### Step 4: Initiate failback

```bash
python cli.py failback initiate
```

This triggers the failback by setting the stream status to `FAILING_OVER`.

### Step 5: Monitor progress

The failback command monitors progress automatically. You can also check manually:

```bash
python cli.py failback status
```

Wait until the stream status is `COMPLETED`.

### Step 6: Redirect traffic

After failback completes, redirect application traffic back to the original primary cluster. If using the proxy layer:

```bash
cd proxy
python integrate.py restore --proxy haproxy
```

## Options

```bash
# Skip replication readiness check (faster but riskier)
python cli.py failback full --no-wait

# Failback to a specific timestamp
python cli.py failback initiate --failover-at 2025-05-01T19:39:39Z

# Skip confirmation prompt
python cli.py failback full --yes
```

## After Failback

1. Verify the original primary is serving traffic correctly
2. Consider creating a new forward PCR stream for future failover capability:
   ```bash
   python cli.py setup
   ```
3. Update monitoring if cluster roles have changed
