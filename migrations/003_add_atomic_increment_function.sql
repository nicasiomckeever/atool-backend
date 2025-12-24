-- Migration 003: Add atomic increment function for generation_count
-- This reduces API calls from 2 to 1 and prevents race conditions

-- Create function to atomically increment generation_count and return new value
CREATE OR REPLACE FUNCTION increment_generation_count(user_uuid UUID)
RETURNS INTEGER AS $$
DECLARE
    new_count INTEGER;
BEGIN
    -- Atomic increment and return new value in one operation
    UPDATE users 
    SET generation_count = COALESCE(generation_count, 0) + 1
    WHERE id = user_uuid
    RETURNING generation_count INTO new_count;
    
    -- Return the new count
    RETURN new_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant execute permission to authenticated users
GRANT EXECUTE ON FUNCTION increment_generation_count(UUID) TO authenticated;
GRANT EXECUTE ON FUNCTION increment_generation_count(UUID) TO service_role;

-- Add comment
COMMENT ON FUNCTION increment_generation_count IS 'Atomically increments user generation_count by 1 and returns new value. Reduces API calls and prevents race conditions.';
