-- Migration 008: Add indexes on foreign keys for better JOIN performance
-- Fixes "unindexed_foreign_keys" performance warnings
-- Improves query performance when joining tables

-- ============================================
-- Add Foreign Key Indexes
-- ============================================

-- Index on priority1_queue.user_id
-- Improves performance when filtering by user or joining with users table
CREATE INDEX IF NOT EXISTS idx_priority1_queue_user_id 
ON priority1_queue(user_id);

-- Index on priority2_queue.user_id
-- Improves performance when filtering by user or joining with users table
CREATE INDEX IF NOT EXISTS idx_priority2_queue_user_id 
ON priority2_queue(user_id);

-- Index on priority3_queue.user_id
-- Improves performance when filtering by user or joining with users table
CREATE INDEX IF NOT EXISTS idx_priority3_queue_user_id 
ON priority3_queue(user_id);

-- Index on usage_logs.job_id
-- Improves performance when querying usage logs by job
CREATE INDEX IF NOT EXISTS idx_usage_logs_job_id 
ON usage_logs(job_id);

-- ============================================
-- Composite Indexes for Common Query Patterns
-- ============================================

-- Composite index for priority queue queries (user + processed status)
-- Useful for querying a user's pending jobs in priority queues
CREATE INDEX IF NOT EXISTS idx_priority1_queue_user_processed 
ON priority1_queue(user_id, processed);

CREATE INDEX IF NOT EXISTS idx_priority2_queue_user_processed 
ON priority2_queue(user_id, processed);

CREATE INDEX IF NOT EXISTS idx_priority3_queue_user_processed 
ON priority3_queue(user_id, processed);

-- Composite index for usage logs (user + timestamp)
-- Useful for querying user's usage history over time
CREATE INDEX IF NOT EXISTS idx_usage_logs_user_created 
ON usage_logs(user_id, created_at DESC);

-- ============================================
-- Add Comments
-- ============================================

COMMENT ON INDEX idx_priority1_queue_user_id IS 'Foreign key index for user lookups and joins';
COMMENT ON INDEX idx_priority2_queue_user_id IS 'Foreign key index for user lookups and joins';
COMMENT ON INDEX idx_priority3_queue_user_id IS 'Foreign key index for user lookups and joins';
COMMENT ON INDEX idx_usage_logs_job_id IS 'Foreign key index for job-related usage log queries';
COMMENT ON INDEX idx_priority1_queue_user_processed IS 'Composite index for user pending job queries';
COMMENT ON INDEX idx_priority2_queue_user_processed IS 'Composite index for user pending job queries';
COMMENT ON INDEX idx_priority3_queue_user_processed IS 'Composite index for user pending job queries';
COMMENT ON INDEX idx_usage_logs_user_created IS 'Composite index for user usage history queries';

-- ============================================
-- Analyze tables to update statistics
-- ============================================

ANALYZE priority1_queue;
ANALYZE priority2_queue;
ANALYZE priority3_queue;
ANALYZE usage_logs;

-- ============================================
-- Verification
-- ============================================

DO $$
DECLARE
    idx_count INTEGER;
BEGIN
    -- Count new indexes
    SELECT COUNT(*) INTO idx_count
    FROM pg_indexes
    WHERE schemaname = 'public'
    AND indexname IN (
        'idx_priority1_queue_user_id',
        'idx_priority2_queue_user_id',
        'idx_priority3_queue_user_id',
        'idx_usage_logs_job_id'
    );
    
    IF idx_count = 4 THEN
        RAISE NOTICE '✅ All 4 foreign key indexes created successfully';
    ELSE
        RAISE WARNING '⚠️ Expected 4 indexes but found %', idx_count;
    END IF;
END $$;
