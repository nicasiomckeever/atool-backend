-- Migration 012: Re-enable RLS with Correct Policies for Realtime
-- This fixes the security warning while keeping Realtime broadcasts working

-- Step 1: Enable RLS on jobs table
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;

-- Step 2: Drop any existing policies (clean slate)
DROP POLICY IF EXISTS "Users can view own jobs" ON jobs;
DROP POLICY IF EXISTS "Users can insert own jobs" ON jobs;
DROP POLICY IF EXISTS "Users can update own jobs" ON jobs;
DROP POLICY IF EXISTS "Users can delete own jobs" ON jobs;
DROP POLICY IF EXISTS "Enable read access for users" ON jobs;
DROP POLICY IF EXISTS "Enable insert access for users" ON jobs;

-- Step 3: Create comprehensive policies that work with Realtime

-- Policy 1: Users can SELECT their own jobs
CREATE POLICY "Users can view own jobs"
ON jobs
FOR SELECT
USING (auth.uid() = user_id);

-- Policy 2: Users can INSERT their own jobs
CREATE POLICY "Users can insert own jobs"
ON jobs
FOR INSERT
WITH CHECK (auth.uid() = user_id);

-- Policy 3: Users can UPDATE their own jobs
CREATE POLICY "Users can update own jobs"
ON jobs
FOR UPDATE
USING (auth.uid() = user_id);

-- Policy 4: Users can DELETE their own jobs
CREATE POLICY "Users can delete own jobs"
ON jobs
FOR DELETE
USING (auth.uid() = user_id);

-- Step 4: Ensure Realtime is enabled with proper replica identity
-- This is CRITICAL for Realtime to broadcast changes
ALTER TABLE jobs REPLICA IDENTITY FULL;

-- Step 5: Grant necessary permissions to authenticated users
GRANT SELECT, INSERT, UPDATE, DELETE ON jobs TO authenticated;
-- Note: No sequence grant needed since jobs.job_id uses UUID, not SERIAL

-- Step 6: Realtime publication verification
-- Note: jobs table is already in supabase_realtime publication from migration 003
-- No action needed here - Realtime is already configured ✓

-- VERIFICATION:
-- Run these queries in Supabase SQL Editor to verify:
-- 
-- 1. Check RLS is enabled:
--    SELECT tablename, rowsecurity FROM pg_tables WHERE tablename = 'jobs';
--    (Should show rowsecurity = true)
--
-- 2. Check policies exist:
--    SELECT * FROM pg_policies WHERE tablename = 'jobs';
--    (Should show 4 policies)
--
-- 3. Check replica identity:
--    SELECT relname, relreplident FROM pg_class WHERE relname = 'jobs';
--    (Should show relreplident = 'f' for FULL)
--
-- 4. Check Realtime publication:
--    SELECT * FROM pg_publication_tables WHERE pubname = 'supabase_realtime';
--    (Should include jobs table)

-- IMPORTANT NOTES:
-- 1. RLS is now ENABLED (secure ✓)
-- 2. Users can only access their own jobs (secure ✓)
-- 3. Realtime broadcasts will work because:
--    - REPLICA IDENTITY is FULL (broadcasts all columns)
--    - Policies use auth.uid() which Realtime respects
--    - Publication includes the jobs table
-- 4. Your frontend Realtime subscription must be authenticated
--    (which it already is via the user session)
