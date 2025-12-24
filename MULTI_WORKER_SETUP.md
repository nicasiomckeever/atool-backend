# Multi-Worker Setup Guide

## Overview
The multi-worker system distributes queue operations across 3 Supabase projects to bypass free-tier limitations and provide redundancy.

## Current Status

### ✅ Configured:
- **Worker 1**: https://gmhpbeqvqpuoctaqgnum.supabase.co
- **Worker 2**: https://weuavzmjqyfjlfzcybtr.supabase.co  
- **Worker 3**: https://zqicjcipwhtovgzxjgjn.supabase.co
- **Edge Function URL**: https://gtgnwrwbcxvasgetfzby.supabase.co/functions/v1/route-queue
- **USE_EDGE_FUNCTION**: `true` (enabled in .env)

### ⚠️ Pending:
- Deploy edge function to Supabase
- Configure environment secrets
- Create queue tables in worker projects

---

## Step 1: Install Supabase CLI

```bash
# Install Supabase CLI
npm install -g supabase

# Verify installation
supabase --version
```

---

## Step 2: Login to Supabase

```bash
supabase login
```

This will open your browser to authenticate.

---

## Step 3: Link to Main Project

```bash
cd C:\Users\RDP\Documents\Atool\backend

# Link to your main Supabase project (gtgnwrwbcxvasgetfzby)
supabase link --project-ref gtgnwrwbcxvasgetfzby
```

---

## Step 4: Set Environment Secrets

The edge function needs worker credentials as secrets:

```bash
# Set Worker 1 credentials (get from backend/.env file)
supabase secrets set WORKER_1_URL=$(grep WORKER_1_URL backend/.env | cut -d '=' -f2)
supabase secrets set WORKER_1_ANON_KEY=$(grep WORKER_1_ANON_KEY backend/.env | cut -d '=' -f2)

# Set Worker 2 credentials (get from backend/.env file)
supabase secrets set WORKER_2_URL=$(grep WORKER_2_URL backend/.env | cut -d '=' -f2)
supabase secrets set WORKER_2_ANON_KEY=$(grep WORKER_2_ANON_KEY backend/.env | cut -d '=' -f2)

# Set Worker 3 credentials (get from backend/.env file)
supabase secrets set WORKER_3_URL=$(grep WORKER_3_URL backend/.env | cut -d '=' -f2)
supabase secrets set WORKER_3_ANON_KEY=$(grep WORKER_3_ANON_KEY backend/.env | cut -d '=' -f2)

# Verify secrets
supabase secrets list
```

**Note**: The above commands automatically read credentials from your `.env` file.

---

## Step 5: Deploy Edge Function

```bash
cd C:\Users\RDP\Documents\Atool\backend

# Deploy the route-queue function
supabase functions deploy route-queue
```

Expected output:
```
Deploying Function route-queue...
✅ Deployed Function route-queue (https://gtgnwrwbcxvasgetfzby.supabase.co/functions/v1/route-queue)
```

---

## Step 6: Create Queue Tables in Worker Projects

Each worker project needs these tables:
- `priority1_queue`
- `priority2_queue`
- `priority3_queue`

### SQL Schema for Each Worker:

```sql
-- Priority 1 Queue
CREATE TABLE IF NOT EXISTS priority1_queue (
    queue_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    job_id UUID NOT NULL,
    request_payload JSONB NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_priority1_queue_processed ON priority1_queue(processed, created_at);
CREATE INDEX idx_priority1_queue_user_id ON priority1_queue(user_id);

-- Priority 2 Queue
CREATE TABLE IF NOT EXISTS priority2_queue (
    queue_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    job_id UUID NOT NULL,
    request_payload JSONB NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_priority2_queue_processed ON priority2_queue(processed, created_at);
CREATE INDEX idx_priority2_queue_user_id ON priority2_queue(user_id);

-- Priority 3 Queue
CREATE TABLE IF NOT EXISTS priority3_queue (
    queue_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    job_id UUID NOT NULL,
    request_payload JSONB NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_priority3_queue_processed ON priority3_queue(processed, created_at);
CREATE INDEX idx_priority3_queue_user_id ON priority3_queue(user_id);
```

**Run this SQL in each worker project:**
1. Go to https://gmhpbeqvqpuoctaqgnum.supabase.co (Worker 1)
2. SQL Editor → New Query → Paste schema → Run
3. Repeat for Worker 2 and Worker 3

---

## Step 7: Test the Setup

```bash
cd C:\Users\RDP\Documents\Atool\backend

# Test the worker client
python worker_client.py
```

Expected output:
```
Testing insert...
Worker client initialized with edge function: https://...
Edge function success. Worker used: worker-1
Inserted job: {...}

Testing select...
✅ Found 1 jobs in queue
First job ID: xxx-xxx-xxx

✅ All tests passed!
```

---

## Step 8: Restart Backend

```bash
cd C:\Users\RDP\Documents\Atool\backend
python app.py
```

The backend will now use the edge function for all queue operations!

---

## Monitoring

### Check Edge Function Logs:
```bash
supabase functions logs route-queue --follow
```

### Check Worker Health:
The edge function automatically tracks worker health:
- Healthy workers: Used in round-robin
- Failed workers: Marked unhealthy for 5 minutes
- Auto-recovery: After 5 minutes, workers are re-tested

---

## Benefits

1. **3x Capacity**: Distribute load across 3 free-tier projects
2. **High Availability**: Automatic failover if a worker fails
3. **Load Balancing**: Round-robin distribution
4. **Free Tier**: All within free Supabase limits

---

## Troubleshooting

### Edge Function Returns 404:
- Function not deployed: Run `supabase functions deploy route-queue`
- Wrong URL: Check `EDGE_FUNCTION_URL` in .env

### Worker Connection Errors:
- Check worker URLs are accessible
- Verify worker API keys are correct
- Ensure queue tables exist in worker projects

### Authentication Errors:
- Use `SUPABASE_SERVICE_ROLE_KEY` (not anon key) for backend
- Set secrets with correct keys

---

## Disable Multi-Worker (Fallback)

If you need to disable the multi-worker system:

1. Edit `backend/.env`:
   ```
   USE_EDGE_FUNCTION=false
   ```

2. Restart backend

The system will fall back to using the main Supabase project directly.
