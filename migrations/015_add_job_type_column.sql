-- Migration 015: Add job_type column to jobs table
-- Purpose: Distinguish between image and video generation jobs
-- This enables proper job recovery after page refresh (show image jobs on image page, video jobs on video page)

-- Add job_type column with default value 'image'
ALTER TABLE jobs 
ADD COLUMN IF NOT EXISTS job_type TEXT DEFAULT 'image';

-- Add check constraint to ensure only valid job types
ALTER TABLE jobs 
ADD CONSTRAINT check_job_type 
CHECK (job_type IN ('image', 'video'));

-- Update existing rows to have job_type = 'image' (for backward compatibility)
UPDATE jobs 
SET job_type = 'image' 
WHERE job_type IS NULL;

-- Make job_type NOT NULL after setting defaults
ALTER TABLE jobs 
ALTER COLUMN job_type SET NOT NULL;

-- Add index for faster queries when filtering by job_type and status
CREATE INDEX IF NOT EXISTS idx_jobs_user_type_status 
ON jobs (user_id, job_type, status, created_at DESC);

-- Add comment for documentation
COMMENT ON COLUMN jobs.job_type IS 'Type of generation job: image or video';

-- Grant necessary permissions (if needed)
-- This ensures the backend can query with job_type filter
-- (Permissions should already exist from initial schema, but included for safety)

-- Verify the migration
DO $$
BEGIN
  -- Check if column exists
  IF EXISTS (
    SELECT 1 
    FROM information_schema.columns 
    WHERE table_name = 'jobs' 
    AND column_name = 'job_type'
  ) THEN
    RAISE NOTICE '✅ Migration 015 successful: job_type column added';
  ELSE
    RAISE EXCEPTION '❌ Migration 015 failed: job_type column not found';
  END IF;
END $$;
