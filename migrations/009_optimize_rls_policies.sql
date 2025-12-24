-- Migration 009: Optimize RLS policies for performance
-- Fixes "auth_rls_initplan" warnings by using (SELECT auth.uid()) pattern
-- This evaluates auth.uid() once per query instead of once per row
-- Significant performance improvement at scale (10x+ faster for large result sets)

-- ============================================
-- USERS TABLE - 2 policies
-- ============================================

DROP POLICY IF EXISTS "Users can view own data" ON users;
CREATE POLICY "Users can view own data" 
    ON users 
    FOR SELECT 
    USING (id = (SELECT auth.uid()));

DROP POLICY IF EXISTS "Users can update own data" ON users;
CREATE POLICY "Users can update own data" 
    ON users 
    FOR UPDATE 
    USING (id = (SELECT auth.uid()));

-- ============================================
-- JOBS TABLE - 4 policies
-- ============================================

DROP POLICY IF EXISTS "Users can view own jobs" ON jobs;
CREATE POLICY "Users can view own jobs" 
    ON jobs 
    FOR SELECT 
    USING (user_id = (SELECT auth.uid()));

DROP POLICY IF EXISTS "Users can create own jobs" ON jobs;
CREATE POLICY "Users can create own jobs" 
    ON jobs 
    FOR INSERT 
    WITH CHECK (user_id = (SELECT auth.uid()));

DROP POLICY IF EXISTS "Users can update own jobs" ON jobs;
CREATE POLICY "Users can update own jobs" 
    ON jobs 
    FOR UPDATE 
    USING (user_id = (SELECT auth.uid()));

DROP POLICY IF EXISTS "Users can delete own jobs" ON jobs;
CREATE POLICY "Users can delete own jobs" 
    ON jobs 
    FOR DELETE 
    USING (user_id = (SELECT auth.uid()));

-- ============================================
-- SESSIONS TABLE - 1 policy
-- ============================================

DROP POLICY IF EXISTS "Users can view own sessions" ON sessions;
CREATE POLICY "Users can view own sessions" 
    ON sessions 
    FOR SELECT 
    USING (user_id = (SELECT auth.uid()));

-- ============================================
-- USAGE_LOGS TABLE - 1 policy
-- ============================================

DROP POLICY IF EXISTS "Users can view own usage logs" ON usage_logs;
CREATE POLICY "Users can view own usage logs" 
    ON usage_logs 
    FOR SELECT 
    USING (user_id = (SELECT auth.uid()));

-- ============================================
-- PRIORITY QUEUES - 3 policies
-- ============================================

DROP POLICY IF EXISTS "Users can view own priority1 entries" ON priority1_queue;
CREATE POLICY "Users can view own priority1 entries" 
    ON priority1_queue 
    FOR SELECT 
    USING (user_id = (SELECT auth.uid()));

DROP POLICY IF EXISTS "Users can view own priority2 entries" ON priority2_queue;
CREATE POLICY "Users can view own priority2 entries" 
    ON priority2_queue 
    FOR SELECT 
    USING (user_id = (SELECT auth.uid()));

DROP POLICY IF EXISTS "Users can view own priority3 entries" ON priority3_queue;
CREATE POLICY "Users can view own priority3 entries" 
    ON priority3_queue 
    FOR SELECT 
    USING (user_id = (SELECT auth.uid()));

-- ============================================
-- MAGIC_LINKS TABLE - 1 policy
-- ============================================

DROP POLICY IF EXISTS "Users can read own valid magic links" ON magic_links;
CREATE POLICY "Users can read own valid magic links" 
    ON magic_links 
    FOR SELECT 
    TO authenticated
    USING (
        email = (SELECT email FROM auth.users WHERE id = (SELECT auth.uid()))
        AND used = false
        AND expires_at > NOW()
    );

-- ============================================
-- Verification
-- ============================================

DO $$
DECLARE
    policy_count INTEGER;
BEGIN
    -- Count recreated policies
    SELECT COUNT(*) INTO policy_count
    FROM pg_policies
    WHERE schemaname = 'public'
    AND policyname IN (
        'Users can view own data',
        'Users can update own data',
        'Users can view own jobs',
        'Users can create own jobs',
        'Users can update own jobs',
        'Users can delete own jobs',
        'Users can view own sessions',
        'Users can view own usage logs',
        'Users can view own priority1 entries',
        'Users can view own priority2 entries',
        'Users can view own priority3 entries',
        'Users can read own valid magic links'
    );
    
    IF policy_count = 12 THEN
        RAISE NOTICE '✅ All 12 RLS policies optimized successfully';
    ELSE
        RAISE WARNING '⚠️ Expected 12 policies but found %', policy_count;
    END IF;
END $$;

-- ============================================
-- Performance Impact
-- ============================================

-- Before: auth.uid() evaluated for EVERY ROW
--   Query returning 100 jobs = 100 auth.uid() calls = SLOW
--
-- After: auth.uid() evaluated ONCE per query
--   Query returning 100 jobs = 1 auth.uid() call = FAST
--
-- Performance improvement: 10-100x faster for queries returning many rows
-- Most noticeable when users have:
--   - Many jobs (>50)
--   - Many usage logs (>100)
--   - Large result sets from any RLS-protected table
