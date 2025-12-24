# Database Migrations

This folder contains SQL migration files for the Atool database schema. Use this guide to set up a new Supabase project or migrate to a different account.

---

## 🚀 Quick Start - New Supabase Setup

**For a fresh Supabase project, run all migrations in order:**

1. Go to your **Supabase Dashboard** → **SQL Editor**
2. Click **New Query**
3. Copy and paste migrations **in order** (001 → 002 → 003 → ... → 012)
4. Click **Run** after each migration
5. Verify each migration succeeded before proceeding to the next

**⚠️ IMPORTANT:** Run migrations in numerical order. Do not skip any!

---

## 📋 Migration Files

| File | Description | Status |
|------|-------------|--------|
| `001_initial_schema.sql` | Initial database schema with users, jobs, and basic tables | ✅ Applied |
| `002_add_priority_queues.sql` | Creates priority queue tables (priority1, priority2, priority3) | ✅ Applied |
| `003_add_atomic_increment_function.sql` | Adds atomic generation count increment function | ✅ Applied |
| `003_enable_realtime.sql` | Enables Supabase Realtime on jobs table | ✅ Applied |
| `004_optimize_priority_queue_batch.sql` | Optimizes priority queue with batch operations | ✅ Applied |
| `005_batch_job_creation.sql` | Creates batch job creation RPC function | ✅ Applied |
| `006_fix_function_security.sql` | Fixes security definer issues on functions | ✅ Applied |
| `007_enable_rls_magic_links.sql` | Enables RLS policies for magic link authentication | ✅ Applied |
| `008_add_foreign_key_indexes.sql` | Adds indexes for foreign keys to improve performance | ✅ Applied |
| `009_optimize_rls_policies.sql` | Optimizes Row Level Security policies | ✅ Applied |
| `010_add_priority_to_batch_job_creation.sql` | Adds priority support to batch job creation | ✅ Applied |
| `011_disable_rls_for_realtime.sql` | Disables RLS on jobs table (enables Realtime) | ✅ Applied |
| `012_enable_rls_with_correct_policies.sql` | **NEW** - Re-enables RLS with correct policies (SECURITY FIX) | ⚠️ REQUIRED |

## How to Apply Migrations

### Option 1: Supabase SQL Editor (Recommended)

1. Go to your Supabase project dashboard
2. Navigate to **SQL Editor** in the left sidebar
3. Click **New Query**
4. Copy and paste the contents of the migration file
5. Click **Run** to execute the migration
6. Verify the migration succeeded (check the output)

### Option 2: Supabase CLI (Advanced)

```bash
# Install Supabase CLI if not already installed
npm install -g supabase

# Initialize Supabase project (if not already done)
supabase init

# Link to your remote project
supabase link --project-ref YOUR_PROJECT_REF

# Apply a specific migration
supabase db push --db-url YOUR_DATABASE_URL < backend/migrations/010_add_priority_to_batch_job_creation.sql
```

### Option 3: Direct PostgreSQL Connection

```bash
# Using psql
psql "YOUR_SUPABASE_DATABASE_URL" < backend/migrations/010_add_priority_to_batch_job_creation.sql
```

## Latest Migration: 010_add_priority_to_batch_job_creation.sql

This migration fixes the priority display issue in the realtime worker by:

1. **Cleaning up duplicate functions** - Removes conflicting versions of `create_job_batch`
2. **Adding priority support** - Updates the batch job creation function to include priority in job metadata
3. **Maintaining atomicity** - Ensures generation count increment and priority calculation happen atomically

### What This Fixes

**Before:**
```json
{'metadata': {}}  // Empty metadata in Realtime INSERT event
```

**After:**
```json
{'metadata': {'priority': 2}}  // Priority included in Realtime INSERT event
```

### Priority Levels

- 🔵 **Priority 1**: Users with ≤10 generations (highest priority)
- 🟡 **Priority 2**: Users with 11-50 generations (medium priority)
- 🟠 **Priority 3**: Users with >50 generations (lowest priority)

### To Apply This Migration

Run this in **Supabase SQL Editor**:

```sql
-- Copy and paste the entire contents of:
-- backend/migrations/010_add_priority_to_batch_job_creation.sql
```

After applying, restart your Flask backend to pick up the changes.

## Verification

After applying migration 010, verify it worked:

```sql
-- Check the function exists
SELECT routine_name, routine_type, data_type 
FROM information_schema.routines 
WHERE routine_name = 'create_job_batch';

-- Test the function (replace with your user UUID)
SELECT create_job_batch(
    'YOUR_USER_UUID'::uuid,
    'Test prompt',
    'flux-dev',
    '1:1'
);

-- Check that the job has priority in metadata
SELECT job_id, metadata 
FROM jobs 
ORDER BY created_at DESC 
LIMIT 1;
```

## Rollback

If you need to rollback migration 010:

```sql
-- This will revert to the traditional method
DROP FUNCTION IF EXISTS create_job_batch(uuid, text, text, text);

-- Then set in your .env:
USE_BATCH_JOB_CREATION=false
```

## Notes

- Always backup your database before applying migrations
- Test migrations in a development environment first
- Migrations should be applied in order (001, 002, 003, etc.)
- Migration 010 is backward compatible - if the RPC fails, it falls back to the traditional method

---

## 📖 Complete Setup Guide for New Supabase Project

Follow these steps to set up Atool on a fresh Supabase account:

### Step 1: Create Supabase Project

1. Go to [supabase.com](https://supabase.com) and sign in
2. Click **"New Project"**
3. Fill in project details:
   - **Name:** Atool (or your preferred name)
   - **Database Password:** Save this securely!
   - **Region:** Choose closest to your users
4. Click **"Create new project"**
5. Wait 2-3 minutes for setup to complete

### Step 2: Get API Credentials

1. In your project, go to **Settings** (gear icon) → **API**
2. Copy these values (you'll need them for `.env`):
   - **Project URL** (e.g., `https://xxxxx.supabase.co`)
   - **anon public** key
   - **service_role** key (keep this secret!)

### Step 3: Apply All Migrations

**Run migrations in this exact order:**

#### Migration 001: Initial Schema
```sql
-- Copy contents of: backend/migrations/001_initial_schema.sql
-- Creates: users, jobs, magic_links, usage_logs tables
```

#### Migration 002: Priority Queues
```sql
-- Copy contents of: backend/migrations/002_add_priority_queues.sql
-- Creates: priority1_queue, priority2_queue, priority3_queue
```

#### Migration 003: Atomic Increment
```sql
-- Copy contents of: backend/migrations/003_add_atomic_increment_function.sql
-- Creates: increment_generation_count() function
```

#### Migration 003b: Enable Realtime
```sql
-- Copy contents of: backend/migrations/003_enable_realtime.sql
-- Enables: Realtime on jobs table
```

#### Migration 004: Optimize Queues
```sql
-- Copy contents of: backend/migrations/004_optimize_priority_queue_batch.sql
-- Creates: get_next_priority_job() function
```

#### Migration 005: Batch Job Creation
```sql
-- Copy contents of: backend/migrations/005_batch_job_creation.sql
-- Creates: create_job_batch() function
```

#### Migration 006: Fix Function Security
```sql
-- Copy contents of: backend/migrations/006_fix_function_security.sql
-- Fixes: SECURITY DEFINER on functions
```

#### Migration 007: Enable RLS for Magic Links
```sql
-- Copy contents of: backend/migrations/007_enable_rls_magic_links.sql
-- Creates: RLS policies for magic_links table
```

#### Migration 008: Add Indexes
```sql
-- Copy contents of: backend/migrations/008_add_foreign_key_indexes.sql
-- Creates: Performance indexes on foreign keys
```

#### Migration 009: Optimize RLS
```sql
-- Copy contents of: backend/migrations/009_optimize_rls_policies.sql
-- Optimizes: Row Level Security policies
```

#### Migration 010: Priority in Batch Creation
```sql
-- Copy contents of: backend/migrations/010_add_priority_to_batch_job_creation.sql
-- Updates: create_job_batch() to include priority metadata
```

#### Migration 011: Disable RLS for Realtime
```sql
-- Copy contents of: backend/migrations/011_disable_rls_for_realtime.sql
-- Disables: RLS on jobs table (required for Realtime to work)
-- Drops: All RLS policies on jobs table
```

### Step 4: Verify Setup

Run these verification queries after all migrations:

```sql
-- 1. Check all tables exist
SELECT tablename 
FROM pg_tables 
WHERE schemaname = 'public' 
ORDER BY tablename;
-- Expected: jobs, magic_links, priority1_queue, priority2_queue, priority3_queue, usage_logs, users

-- 2. Check all functions exist
SELECT routine_name 
FROM information_schema.routines 
WHERE routine_schema = 'public' 
  AND routine_type = 'FUNCTION'
ORDER BY routine_name;
-- Expected: create_job_batch, get_next_priority_job, increment_generation_count

-- 3. Check Realtime is enabled
SELECT tablename 
FROM pg_publication_tables 
WHERE pubname = 'supabase_realtime';
-- Expected: jobs should be in the list

-- 4. Check RLS is disabled on jobs
SELECT tablename, rowsecurity 
FROM pg_tables 
WHERE tablename = 'jobs';
-- Expected: rowsecurity = false

-- 5. Check no policies on jobs table
SELECT count(*) as policy_count 
FROM pg_policies 
WHERE tablename = 'jobs';
-- Expected: policy_count = 0
```

### Step 5: Configure Backend Environment

Update your `.env` file with the new Supabase credentials:

```bash
# Supabase Configuration
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_ANON_KEY=your_anon_key_here
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key_here

# Other settings
UNLIMITED_MODE=true
USE_BATCH_JOB_CREATION=true
```

### Step 6: Configure Frontend Environment

Update your `frontend/.env` file:

```bash
VITE_SUPABASE_URL=https://your-project-id.supabase.co
VITE_SUPABASE_ANON_KEY=your_anon_key_here
```

### Step 7: Test the Setup

1. **Start the backend:**
   ```bash
   cd backend
   python app.py
   ```

2. **Start the frontend:**
   ```bash
   npm run dev
   ```

3. **Start the worker:**
   ```bash
   python backend/job_worker_realtime.py
   ```

4. **Test job creation:**
   - Open the app in your browser
   - Sign in with magic link
   - Create an image generation job
   - Verify Realtime updates work (no polling!)

---

## 🔄 Migrating from Old Supabase to New Supabase

If you're moving from one Supabase account to another:

### Option 1: Fresh Setup (Recommended)

1. Follow the **Complete Setup Guide** above
2. Users will need to sign in again with magic links
3. Old job history will not be transferred

### Option 2: Data Migration (Advanced)

If you need to preserve user data:

```sql
-- 1. Export users from old database
COPY (SELECT * FROM users) TO '/tmp/users.csv' CSV HEADER;

-- 2. Export job history from old database
COPY (SELECT * FROM jobs) TO '/tmp/jobs.csv' CSV HEADER;

-- 3. In new database, after running all migrations:
COPY users FROM '/tmp/users.csv' CSV HEADER;
COPY jobs FROM '/tmp/jobs.csv' CSV HEADER;
```

---

## 🐛 Troubleshooting

### Issue: "Realtime not working"
**Solution:** Verify migration 011 was applied (RLS disabled on jobs table)

### Issue: "Priority not showing in worker logs"
**Solution:** Run migration 010 and restart backend

### Issue: "Function does not exist" errors
**Solution:** Run migrations 003, 004, 005 in order

### Issue: "RLS linter warnings"
**Solution:** Run migration 011 to drop all RLS policies

### Issue: "Permission denied" errors
**Solution:** Make sure you're using SERVICE_ROLE_KEY in backend, not ANON_KEY

---

## 📞 Support

If you encounter issues:
1. Check the troubleshooting section above
2. Verify all migrations ran successfully
3. Check Supabase Dashboard → Database → Logs for errors
4. Review backend console logs for error messages
