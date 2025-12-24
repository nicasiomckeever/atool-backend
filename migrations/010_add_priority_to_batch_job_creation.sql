-- Migration 010: Add Priority Support to Batch Job Creation
-- Description: Updates the create_job_batch function to include priority level in job metadata
-- Author: Droid
-- Date: 2025-11-17

-- ============================================================================
-- CLEANUP: Drop duplicate/old versions of create_job_batch function
-- ============================================================================

-- Drop all existing versions of create_job_batch to avoid conflicts
DROP FUNCTION IF EXISTS create_job_batch(uuid, text, text, text);
DROP FUNCTION IF EXISTS create_job_batch(uuid, text, text, text, integer);
DROP FUNCTION IF EXISTS create_job_batch(p_user_id uuid, p_prompt text, p_model text, p_aspect_ratio text);
DROP FUNCTION IF EXISTS create_job_batch(p_user_id uuid, p_prompt text, p_model text, p_aspect_ratio text, p_generation_threshold integer);

-- ============================================================================
-- CREATE: New batch job creation function with priority support
-- ============================================================================

CREATE OR REPLACE FUNCTION create_job_batch(
    p_user_id uuid,
    p_prompt text,
    p_model text,
    p_aspect_ratio text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER -- Run with the privileges of the function owner
AS $$
DECLARE
    v_credits int;
    v_generation_count int;
    v_new_generation_count int;
    v_job_id uuid;
    v_queue_table text;
    v_priority_level int;
    v_unlimited_mode boolean := true; -- Match UNLIMITED_MODE from backend
BEGIN
    -- ========================================================================
    -- Step 1: Get user credits and generation count
    -- ========================================================================
    SELECT credits, COALESCE(generation_count, 0)
    INTO v_credits, v_generation_count
    FROM users
    WHERE id = p_user_id;
    
    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'success', false, 
            'error', 'User not found'
        );
    END IF;
    
    -- ========================================================================
    -- Step 2: Check credits (skip in unlimited mode)
    -- ========================================================================
    IF NOT v_unlimited_mode AND v_credits < 1 THEN
        RETURN jsonb_build_object(
            'success', false, 
            'error', 'Insufficient credits'
        );
    END IF;
    
    -- ========================================================================
    -- Step 3: Increment generation count atomically
    -- ========================================================================
    UPDATE users
    SET generation_count = generation_count + 1
    WHERE id = p_user_id
    RETURNING generation_count INTO v_new_generation_count;
    
    -- ========================================================================
    -- Step 4: Determine priority based on generation count
    -- Priority 1 (🔵): ≤10 generations  (highest priority)
    -- Priority 2 (🟡): 11-50 generations (medium priority)
    -- Priority 3 (🟠): >50 generations   (lowest priority)
    -- ========================================================================
    IF v_new_generation_count <= 10 THEN
        v_queue_table := 'priority1_queue';
        v_priority_level := 1;
    ELSIF v_new_generation_count <= 50 THEN
        v_queue_table := 'priority2_queue';
        v_priority_level := 2;
    ELSE
        v_queue_table := 'priority3_queue';
        v_priority_level := 3;
    END IF;
    
    -- ========================================================================
    -- Step 5: Create job WITH priority in metadata (for Realtime broadcast)
    -- ========================================================================
    INSERT INTO jobs (
        user_id, 
        prompt, 
        model, 
        aspect_ratio, 
        status, 
        progress, 
        metadata
    )
    VALUES (
        p_user_id, 
        p_prompt, 
        p_model, 
        p_aspect_ratio, 
        'pending', 
        0, 
        jsonb_build_object('priority', v_priority_level) -- ✅ Priority included in INSERT
    )
    RETURNING job_id INTO v_job_id;
    
    -- ========================================================================
    -- Step 6: Insert into appropriate priority queue
    -- ========================================================================
    EXECUTE format(
        'INSERT INTO %I (user_id, job_id, request_payload) VALUES ($1, $2, $3)',
        v_queue_table
    ) USING 
        p_user_id, 
        v_job_id, 
        jsonb_build_object(
            'prompt', p_prompt,
            'model', p_model,
            'aspect_ratio', p_aspect_ratio
        );
    
    -- ========================================================================
    -- Step 7: Deduct credit (skip in unlimited mode)
    -- ========================================================================
    IF NOT v_unlimited_mode THEN
        UPDATE users 
        SET credits = credits - 1 
        WHERE id = p_user_id;
        
        INSERT INTO usage_logs (user_id, job_id, credits_used, action)
        VALUES (p_user_id, v_job_id, 1, 'image_generation');
    END IF;
    
    -- ========================================================================
    -- Step 8: Return success response
    -- ========================================================================
    RETURN jsonb_build_object(
        'success', true,
        'job', jsonb_build_object(
            'id', v_job_id,
            'status', 'pending',
            'progress', 0,
            'priority', v_priority_level,
            'generation_number', v_new_generation_count
        ),
        'credits_remaining', v_credits - 1,
        'priority', v_priority_level,
        'queue_table', v_queue_table
    );
    
EXCEPTION
    WHEN OTHERS THEN
        -- Return error details for debugging
        RETURN jsonb_build_object(
            'success', false,
            'error', SQLERRM,
            'detail', SQLSTATE
        );
END;
$$;

-- ============================================================================
-- GRANT: Ensure authenticated users can call this function
-- ============================================================================

GRANT EXECUTE ON FUNCTION create_job_batch(uuid, text, text, text) TO authenticated;
GRANT EXECUTE ON FUNCTION create_job_batch(uuid, text, text, text) TO service_role;

-- ============================================================================
-- COMMENT: Add documentation
-- ============================================================================

COMMENT ON FUNCTION create_job_batch IS 
'Batch job creation function with priority queue support. 
Creates a job, assigns priority based on user generation count, 
and inserts into appropriate priority queue.
Priority levels: 1 (≤10), 2 (11-50), 3 (>50)';

-- ============================================================================
-- VERIFICATION: Optional query to verify the function was created
-- ============================================================================

-- Run this to verify:
-- SELECT routine_name, routine_type, data_type 
-- FROM information_schema.routines 
-- WHERE routine_name = 'create_job_batch';
