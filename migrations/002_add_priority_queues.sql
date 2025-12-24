-- =====================================================
-- Add Generation Count and Priority Queue System
-- Run this in Supabase SQL Editor
-- =====================================================

-- Add generation_count column to users table
ALTER TABLE users 
ADD COLUMN IF NOT EXISTS generation_count INTEGER DEFAULT 0;

-- Create index for generation_count
CREATE INDEX IF NOT EXISTS idx_users_generation_count ON users(generation_count);

-- =====================================================
-- Table: priority1_queue (for users with ≤10 generations)
-- =====================================================
CREATE TABLE IF NOT EXISTS priority1_queue (
    queue_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    request_payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE
);

-- Indexes for priority1_queue
CREATE INDEX IF NOT EXISTS idx_priority1_queue_processed ON priority1_queue(processed);
CREATE INDEX IF NOT EXISTS idx_priority1_queue_created ON priority1_queue(created_at ASC) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_priority1_queue_job_id ON priority1_queue(job_id);

-- =====================================================
-- Table: priority2_queue (for users with ≤50 generations)
-- =====================================================
CREATE TABLE IF NOT EXISTS priority2_queue (
    queue_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    request_payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE
);

-- Indexes for priority2_queue
CREATE INDEX IF NOT EXISTS idx_priority2_queue_processed ON priority2_queue(processed);
CREATE INDEX IF NOT EXISTS idx_priority2_queue_created ON priority2_queue(created_at ASC) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_priority2_queue_job_id ON priority2_queue(job_id);

-- =====================================================
-- Table: priority3_queue (for users with >50 generations)
-- =====================================================
CREATE TABLE IF NOT EXISTS priority3_queue (
    queue_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    request_payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE
);

-- Indexes for priority3_queue
CREATE INDEX IF NOT EXISTS idx_priority3_queue_processed ON priority3_queue(processed);
CREATE INDEX IF NOT EXISTS idx_priority3_queue_created ON priority3_queue(created_at ASC) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_priority3_queue_job_id ON priority3_queue(job_id);

-- =====================================================
-- Row Level Security (RLS) Policies for Priority Queues
-- =====================================================

-- Enable RLS on priority queue tables
ALTER TABLE priority1_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE priority2_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE priority3_queue ENABLE ROW LEVEL SECURITY;

-- Users can view their own queue entries
CREATE POLICY "Users can view own priority1 entries" ON priority1_queue
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY "Users can view own priority2 entries" ON priority2_queue
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY "Users can view own priority3 entries" ON priority3_queue
    FOR SELECT USING (user_id = auth.uid());

-- Service role can do everything (for backend operations)
-- These policies are implicitly allowed via service_role key

-- =====================================================
-- Grant Permissions
-- =====================================================

GRANT SELECT, INSERT, UPDATE, DELETE ON priority1_queue TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON priority2_queue TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON priority3_queue TO authenticated;

-- =====================================================
-- Completed!
-- =====================================================

-- To verify installation:
-- SELECT table_name FROM information_schema.tables 
-- WHERE table_schema = 'public' 
-- AND table_name LIKE 'priority%_queue';
