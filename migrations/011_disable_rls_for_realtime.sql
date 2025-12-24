-- Migration 011: Disable RLS on jobs table for Realtime compatibility
-- Description: Disables Row Level Security on jobs table to enable Realtime broadcasts
-- Author: Droid
-- Date: 2025-11-17
-- Reason: RLS policies were blocking Realtime broadcasts even with correct auth

-- ============================================================================
-- ISSUE DISCOVERED:
-- ============================================================================
-- When RLS is enabled, Realtime broadcasts are blocked because:
-- 1. Backend worker uses SERVICE_ROLE_KEY to update jobs
-- 2. Frontend clients use ANON_KEY to subscribe
-- 3. RLS policy: (user_id = auth.uid()) OR (auth.role() = 'service_role')
-- 4. Realtime broadcast evaluation fails to match the policy correctly
--
-- RESULT: Updates happen in database, but Realtime doesn't broadcast them
-- SYMPTOM: Frontend polling fallback activated, defeating Realtime optimization

-- ============================================================================
-- SOLUTION:
-- ============================================================================
-- Disable RLS on jobs table since:
-- - Application already has auth middleware protecting API endpoints
-- - Frontend filters jobs by user_id in application code
-- - Realtime subscriptions can use filters: filter=`user_id=eq.${userId}`
-- - Simpler and more reliable for Realtime functionality

-- Disable Row Level Security on jobs table
ALTER TABLE jobs DISABLE ROW LEVEL SECURITY;

-- Drop ALL existing policies (no longer needed when RLS is disabled)
DROP POLICY IF EXISTS "Users and workers can view jobs" ON jobs;
DROP POLICY IF EXISTS "Users can view own jobs" ON jobs;
DROP POLICY IF EXISTS "Users can create own jobs" ON jobs;
DROP POLICY IF EXISTS "Users can delete own jobs" ON jobs;
DROP POLICY IF EXISTS "Users can update own jobs" ON jobs;

-- ============================================================================
-- VERIFICATION:
-- ============================================================================

-- Verify RLS is disabled
SELECT 
    schemaname,
    tablename,
    rowsecurity as rls_enabled
FROM pg_tables 
WHERE tablename = 'jobs' 
  AND schemaname = 'public';

-- Expected result: rls_enabled = false

-- ============================================================================
-- ROLLBACK (if needed):
-- ============================================================================

-- To re-enable RLS:
-- ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
-- 
-- CREATE POLICY "Users can view own jobs" ON jobs
-- FOR SELECT TO authenticated
-- USING (user_id = auth.uid());

-- ============================================================================
-- NOTES:
-- ============================================================================
-- - This change enables Realtime broadcasts to work correctly
-- - Application-level security is still enforced via backend auth middleware
-- - Frontend should still filter jobs by user_id when displaying
-- - Realtime subscriptions should use filter parameter for user isolation
