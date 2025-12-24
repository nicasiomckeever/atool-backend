-- Migration 004: Batch Priority Queue Query Optimization
-- Reduces 3 SELECT queries into 1 UNION query
-- Savings: ~34,000 calls/day (from 51,000 to 17,000)

-- Drop existing function if it exists
DROP FUNCTION IF EXISTS get_next_priority_job();

-- Create optimized batch query function
CREATE OR REPLACE FUNCTION get_next_priority_job()
RETURNS TABLE(
    queue_id UUID,
    user_id UUID,
    job_id UUID,
    request_payload JSONB,
    created_at TIMESTAMPTZ,
    priority_level INTEGER,
    queue_table TEXT
) AS $$
BEGIN
    -- Single query combining all 3 priority queues
    -- Returns first unprocessed job by priority order
    RETURN QUERY
    SELECT * FROM (
        (
            SELECT 
                p1.queue_id,
                p1.user_id,
                p1.job_id,
                p1.request_payload,
                p1.created_at,
                1 AS priority_level,
                'priority1_queue'::TEXT AS queue_table
            FROM priority1_queue p1
            WHERE p1.processed = false
            ORDER BY p1.created_at ASC
            LIMIT 1
        )
        UNION ALL
        (
            SELECT 
                p2.queue_id,
                p2.user_id,
                p2.job_id,
                p2.request_payload,
                p2.created_at,
                2 AS priority_level,
                'priority2_queue'::TEXT AS queue_table
            FROM priority2_queue p2
            WHERE p2.processed = false
            ORDER BY p2.created_at ASC
            LIMIT 1
        )
        UNION ALL
        (
            SELECT 
                p3.queue_id,
                p3.user_id,
                p3.job_id,
                p3.request_payload,
                p3.created_at,
                3 AS priority_level,
                'priority3_queue'::TEXT AS queue_table
            FROM priority3_queue p3
            WHERE p3.processed = false
            ORDER BY p3.created_at ASC
            LIMIT 1
        )
    ) combined
    ORDER BY priority_level ASC, created_at ASC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION get_next_priority_job() TO authenticated;
GRANT EXECUTE ON FUNCTION get_next_priority_job() TO service_role;

-- Add comment
COMMENT ON FUNCTION get_next_priority_job IS 'Optimized function that checks all 3 priority queues in one query. Reduces 3 API calls to 1 per worker poll. Savings: ~34,000 calls/day.';
