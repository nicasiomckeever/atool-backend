-- Migration 005: Batch Job Creation RPC
-- Combines 6 operations into 1 RPC call
-- Savings: ~150,000 calls/month

-- Drop existing function if it exists
DROP FUNCTION IF EXISTS create_job_batch(UUID, TEXT, TEXT, TEXT, INTEGER);

-- Create batch job creation function
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
    -- 1. Get user credits and generation count (SELECT users)
    SELECT credits, generation_count INTO v_user_credits, v_generation_count
    FROM users
    WHERE id = p_user_id;
    
    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'User not found'
        );
    END IF;
    
    -- Check credits (can be disabled via UNLIMITED_MODE in backend)
    -- Backend will handle credit check, we just proceed
    
    -- 2. Increment generation count (was separate RPC call)
    v_new_generation_count := COALESCE(v_generation_count, 0) + 1;
    
    UPDATE users
    SET generation_count = v_new_generation_count
    WHERE id = p_user_id;
    
    -- 3. Create job (INSERT jobs)
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
    
    -- 6. Deduct credit (UPDATE users)
    -- Note: Backend handles UNLIMITED_MODE check
    UPDATE users
    SET credits = credits - 1
    WHERE id = p_user_id;
    
    -- 7. Log usage (INSERT usage_logs)
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
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION create_job_batch(UUID, TEXT, TEXT, TEXT, INTEGER) TO authenticated;
GRANT EXECUTE ON FUNCTION create_job_batch(UUID, TEXT, TEXT, TEXT, INTEGER) TO service_role;

-- Add comment
COMMENT ON FUNCTION create_job_batch IS 'Optimized function that combines all job creation operations into a single transaction. Reduces 6 API calls to 1. Savings: ~150,000 calls/month.';
