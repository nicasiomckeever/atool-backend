-- Migration 006: Fix Function Security - Set search_path
-- Fixes "function_search_path_mutable" security warnings
-- Sets explicit search_path to prevent SQL injection attacks

-- ============================================
-- Step 1: Drop existing functions (with CASCADE to handle overloads)
-- ============================================
DROP FUNCTION IF EXISTS increment_generation_count CASCADE;
DROP FUNCTION IF EXISTS get_next_priority_job CASCADE;
DROP FUNCTION IF EXISTS create_job_batch CASCADE;
DROP FUNCTION IF EXISTS cleanup_expired_magic_links CASCADE;
DROP FUNCTION IF EXISTS cleanup_expired_sessions CASCADE;
DROP FUNCTION IF EXISTS update_user_last_login CASCADE;
DROP FUNCTION IF EXISTS get_user_stats CASCADE;
DROP FUNCTION IF EXISTS update_session_activity CASCADE;

-- ============================================
-- Step 2: Recreate with security fixes
-- ============================================

-- Fix: increment_generation_count
-- ============================================
CREATE FUNCTION increment_generation_count(user_uuid UUID)
RETURNS INTEGER AS $$
DECLARE
    new_count INTEGER;
BEGIN
    UPDATE users
    SET generation_count = COALESCE(generation_count, 0) + 1
    WHERE id = user_uuid
    RETURNING generation_count INTO new_count;
    
    RETURN new_count;
END;
$$ LANGUAGE plpgsql 
SECURITY DEFINER
SET search_path = public;  -- ✅ SECURITY FIX

-- ============================================
-- Fix: get_next_priority_job
-- ============================================
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
$$ LANGUAGE plpgsql 
SECURITY DEFINER
SET search_path = public;  -- ✅ SECURITY FIX

-- ============================================
-- Fix: create_job_batch
-- ============================================
CREATE OR REPLACE FUNCTION create_job_batch(
    p_user_id UUID,
    p_prompt TEXT,
    p_model TEXT DEFAULT 'flux-dev',
    p_aspect_ratio TEXT DEFAULT '1:1',
    p_generation_threshold INTEGER DEFAULT 10
)
RETURNS JSONB AS $$
DECLARE
    v_user_credits INTEGER;
    v_generation_count INTEGER;
    v_new_generation_count INTEGER;
    v_job_id UUID;
    v_priority_level INTEGER;
    v_queue_table TEXT;
    v_result JSONB;
BEGIN
    -- 1. Get user credits and generation count
    SELECT credits, generation_count INTO v_user_credits, v_generation_count
    FROM users
    WHERE id = p_user_id;
    
    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'User not found'
        );
    END IF;
    
    -- 2. Increment generation count
    v_new_generation_count := COALESCE(v_generation_count, 0) + 1;
    
    UPDATE users
    SET generation_count = v_new_generation_count
    WHERE id = p_user_id;
    
    -- 3. Create job
    INSERT INTO jobs (user_id, prompt, model, aspect_ratio, status, progress)
    VALUES (p_user_id, p_prompt, p_model, p_aspect_ratio, 'pending', 0)
    RETURNING job_id INTO v_job_id;
    
    -- 4. Determine priority queue
    IF v_new_generation_count <= 10 THEN
        v_priority_level := 1;
        v_queue_table := 'priority1_queue';
    ELSIF v_new_generation_count <= 50 THEN
        v_priority_level := 2;
        v_queue_table := 'priority2_queue';
    ELSE
        v_priority_level := 3;
        v_queue_table := 'priority3_queue';
    END IF;
    
    -- 5. Insert into priority queue
    EXECUTE format(
        'INSERT INTO %I (user_id, job_id, request_payload) VALUES ($1, $2, $3)',
        v_queue_table
    ) USING p_user_id, v_job_id, jsonb_build_object(
        'prompt', p_prompt,
        'model', p_model,
        'aspect_ratio', p_aspect_ratio
    );
    
    -- 6. Deduct credit
    UPDATE users
    SET credits = credits - 1
    WHERE id = p_user_id;
    
    -- 7. Log usage
    INSERT INTO usage_logs (user_id, job_id, credits_used, action)
    VALUES (p_user_id, v_job_id, 1, 'image_generation');
    
    -- Get updated credits
    SELECT credits INTO v_user_credits
    FROM users
    WHERE id = p_user_id;
    
    -- Build success response
    v_result := jsonb_build_object(
        'success', true,
        'job', jsonb_build_object(
            'id', v_job_id,
            'status', 'pending',
            'progress', 0,
            'prompt', p_prompt,
            'model', p_model,
            'aspect_ratio', p_aspect_ratio,
            'priority', v_priority_level,
            'generation_number', v_new_generation_count
        ),
        'credits_remaining', v_user_credits
    );
    
    RETURN v_result;
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', SQLERRM
        );
END;
$$ LANGUAGE plpgsql 
SECURITY DEFINER
SET search_path = public;  -- ✅ SECURITY FIX

-- ============================================
-- Fix: cleanup_expired_magic_links
-- ============================================
CREATE OR REPLACE FUNCTION cleanup_expired_magic_links()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM magic_links
    WHERE expires_at < NOW()
    OR used = true;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql 
SECURITY DEFINER
SET search_path = public;  -- ✅ SECURITY FIX

-- ============================================
-- Fix: cleanup_expired_sessions
-- ============================================
CREATE OR REPLACE FUNCTION cleanup_expired_sessions()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM sessions
    WHERE expires_at < NOW();
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql 
SECURITY DEFINER
SET search_path = public;  -- ✅ SECURITY FIX

-- ============================================
-- Fix: update_user_last_login
-- ============================================
CREATE OR REPLACE FUNCTION update_user_last_login(user_uuid UUID)
RETURNS VOID AS $$
BEGIN
    UPDATE users
    SET last_login_at = NOW()
    WHERE id = user_uuid;
END;
$$ LANGUAGE plpgsql 
SECURITY DEFINER
SET search_path = public;  -- ✅ SECURITY FIX

-- ============================================
-- Fix: get_user_stats
-- ============================================
CREATE OR REPLACE FUNCTION get_user_stats(user_uuid UUID)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
BEGIN
    SELECT jsonb_build_object(
        'total_generations', COALESCE(generation_count, 0),
        'credits_remaining', COALESCE(credits, 0),
        'total_jobs', (SELECT COUNT(*) FROM jobs WHERE user_id = user_uuid),
        'completed_jobs', (SELECT COUNT(*) FROM jobs WHERE user_id = user_uuid AND status = 'completed')
    ) INTO result
    FROM users
    WHERE id = user_uuid;
    
    RETURN COALESCE(result, jsonb_build_object(
        'total_generations', 0,
        'credits_remaining', 0,
        'total_jobs', 0,
        'completed_jobs', 0
    ));
END;
$$ LANGUAGE plpgsql 
SECURITY DEFINER
SET search_path = public;  -- ✅ SECURITY FIX

-- ============================================
-- Fix: update_session_activity
-- ============================================
CREATE OR REPLACE FUNCTION update_session_activity(session_uuid UUID)
RETURNS VOID AS $$
BEGIN
    UPDATE sessions
    SET last_activity_at = NOW()
    WHERE session_id = session_uuid;
END;
$$ LANGUAGE plpgsql 
SECURITY DEFINER
SET search_path = public;  -- ✅ SECURITY FIX

-- ============================================
-- Verification & Comments
-- ============================================

COMMENT ON FUNCTION increment_generation_count IS 'Atomically increments user generation count. SECURITY: Fixed search_path.';
COMMENT ON FUNCTION get_next_priority_job IS 'Batch query for priority job retrieval. SECURITY: Fixed search_path.';
COMMENT ON FUNCTION create_job_batch IS 'Batch job creation combining 6 operations. SECURITY: Fixed search_path.';
COMMENT ON FUNCTION cleanup_expired_magic_links IS 'Cleanup expired magic links. SECURITY: Fixed search_path.';
COMMENT ON FUNCTION cleanup_expired_sessions IS 'Cleanup expired sessions. SECURITY: Fixed search_path.';
COMMENT ON FUNCTION update_user_last_login IS 'Update user last login timestamp. SECURITY: Fixed search_path.';
COMMENT ON FUNCTION get_user_stats IS 'Get user statistics. SECURITY: Fixed search_path.';
COMMENT ON FUNCTION update_session_activity IS 'Update session activity timestamp. SECURITY: Fixed search_path.';
